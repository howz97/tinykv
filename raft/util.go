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
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"sort"
	"strings"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// IsEmptyHardState returns true if the given HardState is empty.
func IsEmptyHardState(st pb.HardState) bool {
	return isHardStateEqual(st, pb.HardState{})
}

// IsEmptySnap returns true if the given Snapshot is empty.
func IsEmptySnap(sp *pb.Snapshot) bool {
	if sp == nil || sp.Metadata == nil {
		return true
	}
	return sp.Metadata.Index == 0
}

func mustTerm(term uint64, err error) uint64 {
	if err != nil {
		panic(err)
	}
	return term
}

func nodes(r *Raft) []uint64 {
	nodes := make([]uint64, 0, len(r.Prs))
	for id := range r.Prs {
		nodes = append(nodes, id)
	}
	sort.Sort(uint64Slice(nodes))
	return nodes
}

func diffu(a, b string) string {
	if a == b {
		return ""
	}
	aname, bname := mustTemp("base", a), mustTemp("other", b)
	defer os.Remove(aname)
	defer os.Remove(bname)
	cmd := exec.Command("diff", "-u", aname, bname)
	buf, err := cmd.CombinedOutput()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			// do nothing
			return string(buf)
		}
		panic(err)
	}
	return string(buf)
}

func mustTemp(pre, body string) string {
	f, err := ioutil.TempFile("", pre)
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(f, strings.NewReader(body))
	if err != nil {
		panic(err)
	}
	f.Close()
	return f.Name()
}

func ltoa(l *RaftLog) string {
	s := fmt.Sprintf("committed: %d\n", l.committed)
	s += fmt.Sprintf("applied:  %d\n", l.applied)
	for i, e := range l.entries {
		s += fmt.Sprintf("#%d: %+v\n", i, e)
	}
	return s
}

type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

var IsResponseMsg = map[pb.MessageType]bool{
	pb.MessageType_MsgAppendResponse:      true,
	pb.MessageType_MsgPreVoteResp:         true,
	pb.MessageType_MsgRequestVoteResponse: true,
	pb.MessageType_MsgHeartbeatResponse:   true,
}

var IsLocalMsg = map[pb.MessageType]bool{
	pb.MessageType_MsgHup:            true,
	pb.MessageType_MsgBeat:           true,
	pb.MessageType_MsgPropose:        true,
	pb.MessageType_MsgTransferLeader: true, // fixme: this maybe send from follower to leader
}

var OnlyFromLeader = map[pb.MessageType]bool{
	pb.MessageType_MsgHeartbeat:  true,
	pb.MessageType_MsgAppend:     true,
	pb.MessageType_MsgSnapshot:   true,
	pb.MessageType_MsgTimeoutNow: true,
}

func RaftNetMsg(t pb.MessageType) bool {
	return !IsLocalMsg[t]
}

// Be cautious to add new MessageType
var RespMsgOf = map[pb.MessageType]pb.MessageType{
	pb.MessageType_MsgAppend:      pb.MessageType_MsgAppendResponse,
	pb.MessageType_MsgPreVote:     pb.MessageType_MsgPreVoteResp,
	pb.MessageType_MsgRequestVote: pb.MessageType_MsgRequestVoteResponse,
	pb.MessageType_MsgHeartbeat:   pb.MessageType_MsgHeartbeatResponse,
}

func voteType(state StateType) pb.MessageType {
	if state == StatePreCandidate {
		return pb.MessageType_MsgPreVote
	} else if state == StateCandidate {
		return pb.MessageType_MsgRequestVote
	} else {
		panic("invalid state")
	}
}

func isHardStateEqual(a, b pb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

func PEntries(ents []pb.Entry) (pents []*pb.Entry) {
	for i := range ents {
		pents = append(pents, &ents[i])
	}
	return
}

func CheckErr(err error) {
	if err != nil {
		panic(err)
	}
}

func MajorityValue(vals []uint64) uint64 {
	sort.Slice(vals, func(i, j int) bool { return vals[i] > vals[j] })
	return vals[len(vals)/2]
}
