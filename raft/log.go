// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"fmt"
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64
	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64
	// log entries with index <= stabled are persisted to storage.
	stabled uint64
	// uncompacted log entries
	entries []pb.Entry
	// the incoming unstable snapshot, if any.
	pendingSnapshot *pb.Snapshot
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	hs, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}
	lg := &RaftLog{
		storage:   storage,
		committed: hs.Commit,
	}
	first, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lg.applied = first - 1
	last, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	if last >= first {
		ents, err := storage.Entries(first, last+1)
		if err != nil {
			log.Panicf("[first=%d, last=%d): %v", first, last, err)
		}
		lg.entries = ents
	}
	lg.stabled = last
	return lg
}

func (l *RaftLog) String() string {
	return fmt.Sprintf("{%s,app=%d,cmt=%d}",
		EntsStr(l.entries), l.applied, l.committed)
}

func (l *RaftLog) Desc() string {
	return fmt.Sprintf("{%s,stabled=%d}", l, l.stabled)
}

func EntsStr(entries []pb.Entry) string {
	f := func(e pb.Entry) string {
		return fmt.Sprintf("(t%d.i%d)", e.Term, e.Index)
	}
	leng := len(entries)
	ents := fmt.Sprintf("[%d]", leng)
	switch leng {
	case 0:
	case 1:
		ents += f(entries[0])
	case 2:
		ents += f(entries[0])
		ents += f(entries[1])
	default:
		ents += f(entries[0])
		ents += ".."
		ents += f(entries[leng-1])
	}
	return ents
}

func (l *RaftLog) Entries(lo, hi uint64) (ents []pb.Entry, err error) {
	if len(l.entries) == 0 {
		return nil, ErrCompacted
	}
	offset := l.entries[0].Index
	if lo < offset {
		return nil, ErrCompacted
	}
	lo -= offset
	hi -= offset
	cp := make([]pb.Entry, hi-lo)
	copy(cp, l.entries[lo:hi])
	return cp, nil
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	// todo:
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if l.applied == l.committed {
		return nil
	}
	var err error
	ents, err = l.Entries(l.applied+1, l.committed+1)
	if err != nil {
		panic(fmt.Sprintf("entries[%d:%d]: %v", l.applied+1, l.committed+1, err))
	}
	return
}

func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return []pb.Entry{}
	}
	offset := l.entries[0].Index
	uns := l.entries[int64(l.stabled-offset)+1:] // stabled maybe equal to offset-1
	if len(uns) == 0 {
		return []pb.Entry{}
	}
	cp := make([]pb.Entry, len(uns))
	copy(cp, uns)
	return uns
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	last, err := l.storage.LastIndex()
	CheckErr(err)
	return last
}

func (l *RaftLog) LastEntry() (t, i uint64) {
	i = l.LastIndex()
	if i <= 0 {
		return 0, 0
	}
	t, err := l.Term(i)
	CheckErr(err)
	return
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if len(l.entries) == 0 || i < l.entries[0].Index {
		if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
			return l.pendingSnapshot.Metadata.Term, nil
		}
		return l.storage.Term(i)
	}
	i -= l.entries[0].Index
	if i >= uint64(len(l.entries)) {
		return 0, ErrUnavailable
	}
	return l.entries[i].Term, nil
}

func (l *RaftLog) NewerThan(term, idx uint64) bool {
	lt, li := l.LastEntry()
	return lt > term || (lt == term && li > idx)
}
