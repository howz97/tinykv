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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

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

	// unstable entries
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	return nil
}

// log entries with index <= stabled are persisted to storage.
func (l *RaftLog) stabled() uint64 {
	if len(l.entries) > 0 {
		return l.entries[0].Index - 1
	}
	s, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return s
}

func (l *RaftLog) Entries(lo uint64) ([]*pb.Entry, error) {
	var ents []pb.Entry
	var err error
	if lo <= l.stabled() {
		ents, err = l.storage.Entries(lo, l.stabled()+1)
	}
	if err != nil {
		return nil, err
	}
	ents = append(ents, l.entries...)
	var ret []*pb.Entry
	for i := range ents {
		ret = append(ret, &ents[i])
	}
	return ret, nil
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	unstable := len(l.entries)
	if unstable > 0 {
		return l.entries[unstable-1].Index
	}
	last, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return last
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	stabled := l.stabled()
	if i <= stabled {
		return l.storage.Term(i)
	}
	i -= stabled
	i -= 1
	if i >= uint64(len(l.entries)) {
		return 0, ErrUnavailable
	}
	return l.entries[i].Term, nil
}

func (l *RaftLog) NewerThan(term, idx uint64) bool {
	li := l.LastIndex()
	lt, err := l.Term(li)
	if err != nil {
		panic(err)
	}
	return lt > term || (lt == term && li > idx)
}
