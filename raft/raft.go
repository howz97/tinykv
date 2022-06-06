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
	"errors"
	"fmt"
	"math/rand"
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	campaignAfter    int
	// tick advances the internal logical clock by a single tick.
	tick func()

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	hs, _, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	prs := make(map[uint64]*Progress, len(c.peers))
	for _, id := range c.peers {
		prs[id] = &Progress{Match: 0, Next: 1}
	}
	rf := &Raft{
		id:               c.ID,
		RaftLog:          newLog(c.Storage),
		Prs:              prs,
		votes:            make(map[uint64]bool),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	rf.becomeFollower(hs.Term, None)
	rf.Vote = hs.Vote
	// log.Infof("Raft %s start...", rf)
	return rf
}

func (r *Raft) String() string {
	return fmt.Sprintf("[peer%d,t%d,%v]", r.id, r.Term, r.State)
}

func (r *Raft) PrsStr() string {
	str := "{"
	for id, prs := range r.Prs {
		str += fmt.Sprintf("%d%+v, ", id, prs)
	}
	str += "}"
	return str
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	prs := r.Prs[to]
	lt, err := r.RaftLog.Term(prs.Next - 1)
	if err != nil {
		panic(err)
	}
	var ents []pb.Entry
	if prs.Next <= r.RaftLog.LastIndex() {
		ents, err = r.RaftLog.Entries(prs.Next, r.RaftLog.LastIndex()+1)
		if err != nil {
			return false
		}
	}
	var pents []*pb.Entry
	for i := range ents {
		pents = append(pents, &ents[i])
	}
	m := &pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		LogTerm: lt,
		Index:   prs.Next - 1,
		Entries: pents,
	}
	r.sendMsg(to, m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	lt, li := r.RaftLog.LastEntry()
	m := &pb.Message{MsgType: pb.MessageType_MsgHeartbeat, LogTerm: lt, Index: li}
	r.sendMsg(to, m)
}

func (r *Raft) tickNormal() {
	r.campaignAfter--
	if r.campaignAfter <= 0 {
		r.campaignAfter = r.randTimeout()
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			To:      r.id,
			From:    r.id,
			Term:    r.Term,
		})
	}
}

func (r *Raft) randTimeout() int {
	return r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) tickLeader() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	}
}

func (r *Raft) bcastHeartbeat() {
	if r.State != StateLeader {
		return
	}
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.campaignAfter = r.randTimeout()
	r.tick = r.tickNormal
	r.Vote = None
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.Term++
	r.State = StateCandidate
}

func (r *Raft) requestVotes() {
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.Vote = r.id
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	logIdx := r.RaftLog.LastIndex()
	logTerm, err := r.RaftLog.Term(logIdx)
	if err != nil {
		panic(err)
	}
	for to := range r.Prs {
		if to == r.id {
			continue
		}
		m := &pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			LogTerm: logTerm,
			Index:   logIdx,
		}
		r.sendMsg(to, m)
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.tick = r.tickLeader
	r.State = StateLeader
	r.Lead = r.id
	next := r.RaftLog.LastIndex() + 1
	for id, prs := range r.Prs {
		if id == r.id {
			continue
		}
		prs.Match = 0
		prs.Next = next
	}
	// propose no-op entry
	noOp := &pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Data:      nil,
	}
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{noOp},
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// log.Infof("%s Step %+v", r, m)
	if !IsLocalMsg[m.MsgType] {
		if m.Term < r.Term {
			r.onReceiveOldTerm(m)
			return nil
		} else if m.Term > r.Term {
			if IsResponseMsg[m.MsgType] || m.MsgType == pb.MessageType_MsgRequestVote {
				r.becomeFollower(m.Term, None)
			} else {
				r.becomeFollower(m.Term, m.From)
			}
		}
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.requestVotes()
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgPropose:
		if r.State != StateLeader {
			return ErrStepLocalMsg
		}
		r.handlePropse(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartBeatResp(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResp(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResp(m)
	default:
		log.Errorf("can not handle MessageType=%v", m.MsgType)
	}
	return nil
}

func (r *Raft) handlePropse(m pb.Message) {
	index := r.RaftLog.LastIndex() + 1
	for i, e := range m.Entries {
		e.Term = r.Term
		e.Index = index + uint64(i)
		r.RaftLog.entries = append(r.RaftLog.entries, *e)
	}
	selfPrs := r.Prs[r.id]
	selfPrs.Match = r.RaftLog.LastIndex()
	selfPrs.Next = selfPrs.Match + 1
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
		return
	}
	r.bcastAppend()
}

func (r *Raft) bcastAppend() {
	for to := range r.Prs {
		if to == r.id {
			continue
		}
		r.sendAppend(to)
	}
}

func (r *Raft) onReceiveOldTerm(m pb.Message) {
	respTyp, ok := RespMsgOf[m.MsgType]
	if !ok {
		return
	}
	resp := &pb.Message{
		MsgType: respTyp,
	}
	r.sendMsg(m.From, resp)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	if r.State != StateFollower {
		r.becomeFollower(m.Term, m.From)
	}
	r.Lead = m.From
	resp := &pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
	}
	index := m.Index
	term, err := r.RaftLog.Term(index)
	if err != nil {
		if err != ErrUnavailable {
			panic(err)
		}
		index = r.RaftLog.LastIndex()
		term, _ = r.RaftLog.Term(resp.Index)
		err = nil
	}
	for term > m.LogTerm {
		index--
		term, err = r.RaftLog.Term(index)
		if err != nil {
			panic(err)
		}
	}
	if !(term == m.LogTerm && index == m.Index) {
		resp.Reject = true
		resp.LogTerm = term
		resp.Index = index
	} else {
		resp.Reject = false
		if len(r.RaftLog.entries) > 0 {
			// skip entries already match
			for i := 0; i < len(m.Entries); i++ {
				ent := m.Entries[0]
				t, err := r.RaftLog.Term(index + 1)
				if err != nil || ent.Term != t {
					break
				}
				index++
				term = t
				m.Entries = m.Entries[1:]
			}
			// truncate unmatch entries
			if len(m.Entries) > 0 {
				offset := r.RaftLog.entries[0].Index
				r.RaftLog.entries = r.RaftLog.entries[:index+1-offset]
				if index < r.RaftLog.stabled {
					r.RaftLog.stabled = index
				}
			}
		}
		// append entries
		if len(m.Entries) > 0 {
			for _, e := range m.Entries {
				r.RaftLog.entries = append(r.RaftLog.entries, *e)
			}
			resp.Index = r.RaftLog.LastIndex()
		} else {
			// can not assure whether tail logs match with leader
			resp.Index = index
		}
		// sync commit index
		if m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = min(m.Commit, resp.Index)
		}
	}
	r.sendMsg(m.From, resp)
}

func (r *Raft) handleAppendResp(resp pb.Message) {
	if r.State != StateLeader {
		return
	}
	prs := r.Prs[resp.From]
	if !resp.Reject {
		if resp.Index > prs.Match {
			prs.Match = resp.Index
			prs.Next = prs.Match + 1
			if r.maybeCommit() {
				// broadcast commit index
				r.bcastAppend()
			}
		}
		return
	}
	// probing already finished
	if prs.Match > 0 {
		return
	}
	index := resp.Index
	term, err := r.RaftLog.Term(index)
	if err != nil {
		// todo: sendSnapshot
		return
	}
	for term > resp.LogTerm {
		index--
		term, err = r.RaftLog.Term(index)
		if err != nil {
			// todo: sendSnapshot
			return
		}
	}
	prs.Next = index + 1
	r.sendAppend(resp.From)
}

func (r *Raft) maybeCommit() bool {
	var matchs []int
	for _, prs := range r.Prs {
		matchs = append(matchs, int(prs.Match))
	}
	sort.Sort(sort.Reverse(sort.IntSlice(matchs)))
	commit := uint64(matchs[len(r.Prs)/2])
	if commit <= r.RaftLog.committed {
		return false
	}
	t, err := r.RaftLog.Term(commit)
	if err != nil {
		panic(err)
	}
	if t < r.Term {
		return false
	}
	r.RaftLog.committed = commit
	return true
}

func (r *Raft) sendMsg(to uint64, m *pb.Message) {
	m.From = r.id
	m.To = to
	m.Term = r.Term
	m.Commit = r.RaftLog.committed
	r.msgs = append(r.msgs, *m)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	r.Lead = m.From
	r.campaignAfter = r.randTimeout()
	resp := &pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
	}
	lt, err := r.RaftLog.Term(m.Index)
	resp.Reject = (err != nil || lt != m.LogTerm)
	r.sendMsg(m.From, resp)
}

func (r *Raft) handleHeartBeatResp(m pb.Message) {
	if m.Reject {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	resp := &pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
	}
	if r.Vote == None {
		resp.Reject = r.RaftLog.NewerThan(m.LogTerm, m.Index)
		if !resp.Reject {
			r.Vote = m.From
		}
	} else {
		resp.Reject = r.Vote != m.From
	}
	r.sendMsg(m.From, resp)
}

func (r *Raft) handleRequestVoteResp(m pb.Message) {
	if r.State != StateCandidate {
		return
	}
	r.votes[m.From] = !m.Reject
	n := r.voteNum()
	if n > len(r.Prs)/2 {
		r.becomeLeader()
	} else if len(r.votes)-n > len(r.Prs)/2 {
		r.becomeFollower(r.Term, None)
	}
}

func (r *Raft) voteNum() (n int) {
	for _, g := range r.votes {
		if g {
			n++
		}
	}
	return
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
