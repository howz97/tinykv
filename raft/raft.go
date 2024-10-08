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

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

var ErrLeadTransfering = errors.New("leader transfering is in progress")

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StatePreCandidate
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StatePreCandidate",
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
	BeatAck     uint64
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
	// number of ticks since it received last heartbeat from leader
	withoutHeartbeat int
	campaignAfter    int
	// tick advances the internal logical clock by a single tick.
	tick           func()
	heartbeatRound uint64
	step           func(pb.Message) error

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
	readOnly         *readOnly
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	peers := c.peers
	if len(peers) == 0 {
		peers = confState.Nodes
	}
	prs := make(map[uint64]*Progress, len(peers))
	for _, id := range peers {
		prs[id] = &Progress{Match: 0, Next: 1}
	}
	// initialize log structure
	lg := newLog(c.Storage)
	if c.Applied > lg.applied {
		lg.applied = c.Applied
	}

	rf := &Raft{
		id:               c.ID,
		RaftLog:          lg,
		Prs:              prs,
		votes:            make(map[uint64]bool),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		withoutHeartbeat: c.ElectionTick,
	}
	rf.becomeFollower(hardState.Term, None)
	rf.Vote = hardState.Vote
	log.Infof("Raft %s start...", rf)
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
		return r.sendSnapshot(to)
	}
	var ents []pb.Entry
	if prs.Next <= r.RaftLog.LastIndex() {
		ents, err = r.RaftLog.Entries(prs.Next, r.RaftLog.LastIndex()+1)
		if err != nil {
			return r.sendSnapshot(to)
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
	log.Debugf("%s try append entries to peer%d, preMatch=(t%d,i%d), %d entries", r, to, lt, prs.Next-1, len(pents))
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64, round uint64) {
	m := &pb.Message{MsgType: pb.MessageType_MsgHeartbeat, CtxNum: round}
	r.sendMsg(to, m)
}

func (r *Raft) tickNormal() {
	r.withoutHeartbeat++
	r.campaignAfter--
	if r.campaignAfter <= 0 {
		r.campaignAfter = r.randTimeout()
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			To:      r.id,
			From:    r.id,
			Term:    r.Term,
		})
	} else {
		if r.State == StatePreCandidate || r.State == StateCandidate {
			r.maybeResendRequestVote()
		}
	}
}

func (r *Raft) maybeResendRequestVote() {
	interval := r.electionTimeout / 3
	if r.campaignAfter%interval == 0 {
		logTerm, logIdx := r.RaftLog.LastEntry()
		m := &pb.Message{
			MsgType: voteType(r.State),
			LogTerm: logTerm,
			Index:   logIdx,
		}
		for to := range r.Prs {
			if to == r.id {
				continue
			}
			if _, ok := r.votes[to]; ok {
				continue
			}
			// resend request vote
			r.sendMsg(to, m)
			log.Infof("%s did not received vote response from %d, curVotes=%v(size=%d), resending...",
				r, to, r.votes, len(r.Prs))
		}
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
	r.heartbeatRound++
	for id := range r.Prs {
		if id == r.id {
			r.Prs[id].BeatAck = r.heartbeatRound
			continue
		}
		r.sendHeartbeat(id, r.heartbeatRound)
	}
	log.Debugf("%s broadcast heartbeat, round=%d prs=%s", r, r.heartbeatRound, r.ProgressStr())
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	bef := r.String()
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = lead
	r.tick = r.tickNormal
	r.step = r.stepFollower
	r.leadTransferee = None
	r.campaignAfter = r.randTimeout()
	if r.readOnly != nil {
		log.Warnf("%s become follower but, read-only has %d,%d requests",
			r, len(r.readOnly.processing), len(r.readOnly.nextBatch))
		ro := r.readOnly
		ro.processing = append(ro.processing, ro.nextBatch...)
		for _, cmd := range ro.processing {
			cmd.Callback.Done(nil)
		}
		r.readOnly = nil
	}
	log.Infof("raft state transfer: %s -> %s, campaignAfter=%v", bef, r, r.campaignAfter)
}

func (r *Raft) becomePreCandidate() {
	bef := r.String()
	r.State = StatePreCandidate
	r.step = r.stepPreCandidate
	r.tick = r.tickNormal
	// use short electionInterval to retry quickly under unreliable network condition
	r.campaignAfter = r.randTimeout()
	log.Infof("raft state transfer: %s -> %s, campaignAfter=%v", bef, r, r.campaignAfter)
	r.preVotes()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	bef := r.String()
	r.Term++
	r.State = StateCandidate
	r.step = r.stepCandidate
	r.tick = r.tickNormal
	r.Lead = None
	r.Vote = None
	r.campaignAfter = r.randTimeout()
	log.Infof("raft state transfer: %s -> %s, campaignAfter=%v", bef, r, r.campaignAfter)
}

func (r *Raft) preVotes() {
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	if len(r.Prs) == 1 {
		r.becomeCandidate()
		r.requestVotes(false)
		return
	}
	logTerm, logIdx := r.RaftLog.LastEntry()
	for to := range r.Prs {
		if to == r.id {
			continue
		}
		m := &pb.Message{
			MsgType: pb.MessageType_MsgPreVote,
			LogTerm: logTerm,
			Index:   logIdx,
		}
		r.sendMsg(to, m)
	}
}

func (r *Raft) requestVotes(force bool) {
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.Vote = r.id
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	logTerm, logIdx := r.RaftLog.LastEntry()
	for to := range r.Prs {
		if to == r.id {
			continue
		}
		m := &pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			LogTerm: logTerm,
			Index:   logIdx,
			Force:   force,
		}
		r.sendMsg(to, m)
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	bef := r.String()
	r.tick = r.tickLeader
	r.step = r.stepLeader
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
	r.readOnly = new(readOnly)
	// propose no-op entry
	noOp := &pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Data:      nil,
	}
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{noOp},
	})
	log.Infof("raft state transfer: %s -> %s, GroupSize=%d, votes=%v", bef, r, len(r.Prs), r.votes)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	if RaftNetMsg(m.MsgType) {
		if m.Term < r.Term {
			r.onReceiveOldTerm(m)
			return nil
		} else if m.Term > r.Term {
			// bigger term found
			r.becomeFollower(m.Term, None)
			if IsResponseMsg[m.MsgType] {
				// follower ignore response message
				return nil
			}
		}
	}
	// m.Term == r.Term
	if OnlyFromLeader[m.MsgType] {
		// message from leader, and handled by follower
		r.withoutHeartbeat = 0
		switch r.State {
		case StateFollower:
			r.Lead = m.From
		case StatePreCandidate, StateCandidate:
			r.becomeFollower(m.Term, m.From)
		default:
			panic("unexpected role")
		}
	}
	return r.step(m)
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if m.Force {
			r.becomeCandidate()
			r.requestVotes(true)
		} else {
			r.becomePreCandidate()
		}
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgPreVote, pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		r.sendMsg(r.Lead, &m)
	case pb.MessageType_MsgTimeoutNow:
		if _, ok := r.Prs[r.id]; ok {
			log.Infof("%s received MsgTimeoutNow from %v, log=%v", r, m.From, r.RaftLog)
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, Force: true})
		}
	default:
		log.Errorf("%s can not handle MessageType=%v", r, m.MsgType)
	}
	return nil
}

func (r *Raft) stepPreCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomePreCandidate()
	case pb.MessageType_MsgPreVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgPreVoteResp:
		r.handleRequestVoteResp(m)
	default:
		log.Errorf("%s can not handle MessageType=%v", r, m.MsgType)
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomePreCandidate()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResp(m)
	default:
		log.Errorf("%s can not handle MessageType=%v", r, m.MsgType)
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) (err error) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartBeatResp(m)
	case pb.MessageType_MsgPropose:
		err = r.handlePropse(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResp(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		// already be leader
	case pb.MessageType_MsgTransferLeader:
		err = r.handleTransferLeader(m)
	default:
		log.Errorf("%s can not handle MessageType=%v from (peer%d,term=%d)",
			r, m.MsgType, m.From, m.Term)
	}
	return
}

func (r *Raft) handleTransferLeader(m pb.Message) error {
	_, ok := r.Prs[m.From]
	if !ok {
		return ErrStepPeerNotFound
	}
	if m.From == r.id {
		log.Errorf("already be leader, do not need to transfer leader to self")
		return nil
	}
	if r.leadTransferee != None {
		log.Warnf("%s is transfering leader to %d, change to transfer to %d", r, r.leadTransferee, m.From)
	}
	r.leadTransferee = m.From
	if r.matchAllLog(r.leadTransferee) {
		r.sendTimeout()
	} else {
		r.sendAppend(r.leadTransferee)
	}
	log.Infof("%s start transfer leader to %d", r, r.leadTransferee)
	return nil
}

func (r *Raft) sendTimeout() {
	r.sendMsg(r.leadTransferee, &pb.Message{MsgType: pb.MessageType_MsgTimeoutNow})
}

func (r *Raft) matchAllLog(to uint64) bool {
	prs := r.Prs[to]
	return prs.Match == r.RaftLog.LastIndex()
}

func (r *Raft) handlePropse(m pb.Message) error {
	if r.leadTransferee != None {
		log.Warnf("%s reject proposal when transfering leader to %d", r, r.leadTransferee)
		return ErrLeadTransfering
	}
	index := r.RaftLog.LastIndex() + 1
	// record PendingConfIndex to assure config change do not overlap
	if m.Entries[0].EntryType == pb.EntryType_EntryConfChange {
		if len(m.Entries) != 1 {
			panic("too many config change at a time")
		}
		if r.IsConfChanging() {
			log.Warnf("%s proposal config change but pending(%d) not finish", r, r.PendingConfIndex)
			return ErrProposalDropped
		}
		r.PendingConfIndex = index
		log.Infof("%s proposal config change", r)
	}
	// append logs
	for i, e := range m.Entries {
		e.Term = r.Term
		e.Index = index + uint64(i)
		r.RaftLog.entries = append(r.RaftLog.entries, *e)
		log.Infof("%s proposal (t%d,i%d)", r, e.Term, e.Index)
	}
	selfPrs := r.Prs[r.id]
	selfPrs.Match = r.RaftLog.LastIndex()
	selfPrs.Next = selfPrs.Match + 1
	if len(r.Prs) == 1 {
		r.leaderMaybeCommit()
	} else {
		r.bcastAppend()
	}
	return nil
}

func (r *Raft) IsConfChanging() bool {
	return r.PendingConfIndex > r.RaftLog.applied
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
		Reject:  true,
	}
	r.sendMsg(m.From, resp)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	log.Debugf("%s received AppendEntry request PreMatch=(t%d,i%d), len(ents)=%d, followerLog=%s",
		r, m.LogTerm, m.Index, len(m.Entries), r.RaftLog)
	resp := &pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
	}
	index := m.Index
	term, err := r.RaftLog.Term(index)
	if err != nil {
		term, index = r.RaftLog.LastEntry()
	}
	for term > m.LogTerm {
		index--
		term, err = r.RaftLog.Term(index)
		if err != nil {
			log.Errorf("%s, log=%v, ignore stale append entries %v", r, r.RaftLog, m)
			return
		}
	}
	if !(term == m.LogTerm && index == m.Index) {
		resp.Reject = true
		resp.LogTerm = term
		resp.Index = index
		log.Infof("%s reject AppendEntry. hit=(t%d,i%d)", r, term, index)
	} else {
		bef := r.RaftLog.String()
		resp.Reject = false
		if len(r.RaftLog.entries) > 0 {
			// skip entries already match
			for len(m.Entries) > 0 {
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
			log.Infof("%s accept append entries: %s -> %s", r, bef, r.RaftLog)
		} else {
			// can not assure whether tail logs match with leader
			resp.Index = index
		}
		// sync commit index
		if m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = min(m.Commit, resp.Index)
			log.Infof("%s follower updated commited index %d", r, r.RaftLog.committed)
		}
	}
	r.sendMsg(m.From, resp)
}

func (r *Raft) handleAppendResp(resp pb.Message) {
	prs := r.Prs[resp.From]
	if prs == nil {
		log.Warnf("%s received stale message from removed peer, %v", r, resp)
		return
	}
	if !resp.Reject {
		if resp.Index > prs.Match {
			r.leaderUpdateMatch(prs, resp.Index)
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
		r.sendSnapshot(resp.From)
		return
	}
	for term > resp.LogTerm {
		index--
		term, err = r.RaftLog.Term(index)
		if err != nil {
			r.sendSnapshot(resp.From)
			return
		}
	}
	prs.Next = index + 1
	log.Infof("%s probing Next index %d for peer%d", r, prs.Next, resp.From)
	r.sendAppend(resp.From)
}

func (r *Raft) leaderUpdateMatch(prs *Progress, match uint64) {
	prs.Match = match
	prs.Next = prs.Match + 1
	if r.leaderMaybeCommit() {
		// broadcast commit index
		r.bcastAppend()
	}
	if r.leadTransferee != None && r.matchAllLog(r.leadTransferee) {
		r.sendTimeout()
	}
}

func (r *Raft) sendSnapshot(to uint64) bool {
	snap, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		log.Warnf("%s did not sendSnapshot to %d: %v", r, to, err)
		return false
	}
	r.sendMsg(to, &pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		Snapshot: &snap,
	})
	// avoid to send snapshot again immediately
	r.Prs[to].Next = snap.Metadata.Index + 1
	log.Infof("%s sendSnapshot to %d size=(%d), Meta=%s", r, to, len(snap.Data), snap.Metadata.String())
	return true
}

func (r *Raft) leaderMaybeCommit() bool {
	if len(r.Prs) == 0 {
		return false
	}
	var matchs []uint64
	for _, prs := range r.Prs {
		matchs = append(matchs, prs.Match)
	}
	commit := MajorityValue(matchs)
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
	log.Infof("%s leader updated commited index %s", r, r.RaftLog)
	return true
}

func (r *Raft) sendMsg(to uint64, m *pb.Message) {
	m.From = r.id
	m.To = to
	m.Term = r.Term
	m.Commit = r.RaftLog.committed
	if m.MsgType == pb.MessageType_MsgHeartbeat {
		// heartbeat with commit=0 will notify store to replicate peer
		m.Commit = 0
	}
	r.msgs = append(r.msgs, *m)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	r.withoutHeartbeat = 0
	r.campaignAfter = r.randTimeout()
	resp := &pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		CtxNum:  m.CtxNum,
	}
	resp.Term, resp.Index = r.RaftLog.LastEntry()
	r.sendMsg(m.From, resp)
	log.Debugf("%s received heartbeat from leader%d, round=%d, campaignAfter=%v", r, m.From, m.CtxNum, r.campaignAfter)
}

func (r *Raft) handleHeartBeatResp(m pb.Message) {
	prs := r.Prs[m.From]
	if prs == nil {
		return
	}
	log.Debugf("%s log=%s, prs=%s handle heart beat response %s", r, r.RaftLog, r.ProgressStr(), m.String())
	t, err := r.RaftLog.Term(m.Index)
	if err == nil && t == m.LogTerm && prs.Match < m.Index {
		// prs.match may miss update if message AppendResp dropped and no new entry proposaled
		r.leaderUpdateMatch(prs, m.Index)
	}
	if r.RaftLog.NewerThan(m.LogTerm, m.Index) {
		// continue to append entries to follower if message Append or AppendResp dropped
		r.sendAppend(m.From)
	}
	if m.From == r.leadTransferee && r.matchAllLog(m.From) {
		// resend TimeoutNow to avoid blocking proposal new entry
		r.sendTimeout()
	}
	if m.CtxNum > prs.BeatAck {
		prs.BeatAck = m.CtxNum
		log.Debugf("%s updated BeatAck=%d of peer%d, prs=%s", r, prs.BeatAck, m.From, r.ProgressStr())
	}
}

func (r *Raft) ProgressStr() string {
	str := "["
	for id, p := range r.Prs {
		str += fmt.Sprintf("(peer%d,m=%d,ack=%d)", id, p.Match, p.BeatAck)
	}
	str += "]"
	return str
}

// m.MsgType is MessageType_MsgRequestVote or MessageType_MsgPreVote
func (r *Raft) handleRequestVote(m pb.Message) {
	resp := &pb.Message{
		MsgType: RespMsgOf[m.MsgType],
	}
	if m.MsgType == pb.MessageType_MsgRequestVote && r.Vote != None {
		log.Infof("%s handleRequestVote %v from %v, r.Vote=%v Reject=%v", r, m.MsgType, m.From, r.Vote, r.Vote != m.From)
		resp.Reject = r.Vote != m.From
		r.sendMsg(m.From, resp)
		return
	}
	// did not vote until now, so check vote condition
	if r.RaftLog.NewerThan(m.LogTerm, m.Index) {
		log.Infof("%s handleRequestVote %v from %v, candidateLog=(t%d,i%d), Reject=true", r, m.MsgType, m.From, m.LogTerm, m.Index)
		resp.Reject = true
		r.sendMsg(m.From, resp)
		return
	}
	if !m.Force && r.withoutHeartbeat < r.electionTimeout {
		log.Infof("%s handleRequestVote %v reject vote for %d, because it just received heartbeat at last %d(<%d) tick",
			r, m.MsgType, m.From, r.withoutHeartbeat, r.electionTimeout)
		resp.Reject = true
		r.sendMsg(m.From, resp)
		return
	}
	resp.Reject = false
	if m.MsgType == pb.MessageType_MsgRequestVote {
		r.Vote = m.From
	}
	r.sendMsg(m.From, resp)
	log.Infof("%s handleRequestVote, r.Vote=%v, r.withoutBeat=%d, r.Log=%v: granted vote to %s", r, r.Vote, r.withoutHeartbeat, r.RaftLog, m.String())
}

// m.MsgType is MessageType_MsgRequestVoteResponse or MessageType_MsgPreVoteResp
func (r *Raft) handleRequestVoteResp(m pb.Message) {
	r.votes[m.From] = !m.Reject
	n := r.voteNum()
	log.Infof("%s handleRequestVoteResp %v from peer%v, reject=%v, current votes=%v, goupSize=%d",
		r, m.MsgType, m.From, m.Reject, r.votes, len(r.Prs))
	if n > len(r.Prs)/2 {
		if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
			r.becomeLeader()
		} else {
			r.becomeCandidate()
			r.requestVotes(false)
		}
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

func (r *Raft) ready() Ready {
	rd := Ready{
		Entries:          r.RaftLog.unstableEntries(),
		CommittedEntries: r.RaftLog.nextEnts(),
		ReadOnly:         r.readyReadOnly(),
	}
	if r.RaftLog.pendingSnapshot != nil {
		rd.Snapshot = *r.RaftLog.pendingSnapshot
	}
	// readyReadOnly may broadcast heartbeats, so rd.Messages handled at last
	rd.Messages, r.msgs = r.msgs, nil
	return rd
}

func (r *Raft) hasReady(hs pb.HardState) bool {
	return len(r.msgs) > 0 || r.RaftLog.committed > r.RaftLog.applied ||
		r.RaftLog.pendingSnapshot != nil || r.RaftLog.stabled < r.RaftLog.LastIndex() ||
		hs.Term != r.Term || hs.Commit != r.RaftLog.committed || hs.Vote != r.Vote ||
		r.isReadOnlyReady()
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	cmptIdx := m.Snapshot.Metadata.Index
	if cmptIdx <= r.RaftLog.committed {
		return
	}
	if r.RaftLog.pendingSnapshot != nil {
		return
	}
	log.Infof("%s handle snapshot(%d) from %d .", r, cmptIdx, m.From)
	r.RaftLog.pendingSnapshot = m.Snapshot
	if len(r.RaftLog.entries) > 0 {
		offset := cmptIdx - r.RaftLog.entries[0].Index + 1
		if offset < uint64(len(r.RaftLog.entries)) {
			num := copy(r.RaftLog.entries, r.RaftLog.entries[offset:])
			r.RaftLog.entries = r.RaftLog.entries[:num]
		} else {
			r.RaftLog.entries = nil
		}
	}
	if cmptIdx > r.RaftLog.stabled {
		r.RaftLog.stabled = cmptIdx
	}
	r.RaftLog.committed = cmptIdx
	r.RaftLog.applied = cmptIdx

	// update members
	r.Prs = make(map[uint64]*Progress, len(m.Snapshot.Metadata.ConfState.Nodes))
	for _, id := range m.Snapshot.Metadata.ConfState.Nodes {
		r.Prs[id] = new(Progress)
	}
}

func (r *Raft) advance(rd Ready) {
	if len(rd.Entries) > 0 {
		r.RaftLog.stabled = rd.Entries[len(rd.Entries)-1].Index
	}
	if len(rd.CommittedEntries) > 0 {
		r.RaftLog.applied = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
	}
	if !IsEmptySnap(&rd.Snapshot) {
		r.RaftLog.pendingSnapshot = nil
		if rd.Snapshot.Metadata.Index > r.RaftLog.stabled {
			r.RaftLog.stabled = rd.Snapshot.Metadata.Index
		}
	}
	if len(rd.CommittedEntries) > 0 || !IsEmptySnap(&rd.Snapshot) {
		log.Infof("%s advance: log=%s", r, r.RaftLog.Desc())
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	r.Prs[id] = new(Progress)
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	if r.leadTransferee == id {
		// abort transfer leader if transferee is removed
		r.leadTransferee = None
	}
	delete(r.Prs, id)
	if len(r.Prs) == 0 {
		log.Warnf("group has 0 member")
		return
	}
	if r.State == StateLeader {
		if r.leaderMaybeCommit() {
			// broadcast commit index
			r.bcastAppend()
		}
	}
}

func (r *Raft) ChooseTransferee() uint64 {
	if r.State != StateLeader {
		panic("only leader can call this")
	}
	transferee := None
	// choose the most match follower
	for id, prs := range r.Prs {
		if id == r.id {
			continue
		}
		if transferee == None {
			transferee = id
			continue
		}
		if prs.Match > r.Prs[transferee].Match {
			transferee = id
		}
	}
	return transferee
}

func (r *Raft) GetTransferee() uint64 {
	return r.leadTransferee
}

func (r *Raft) GetLagger() (lagger []uint64) {
	for id, prs := range r.Prs {
		if prs.Match == 0 {
			lagger = append(lagger, id)
		}
	}
	return lagger
}

func (r *Raft) proposalRead(cmd *message.MsgRaftCmd) {
	// accumulate queries
	r.readOnly.nextBatch = append(r.readOnly.nextBatch, cmd)
	log.Infof("%s readOnly=%s, accumulated query %v", r, r.readOnly, cmd.Request)
}

func (r *Raft) readyReadOnly() []*message.MsgRaftCmd {
	if !r.isReadOnlyReady() {
		return nil
	}
	readOnly := r.readOnly
	rd := r.readOnly.processing
	readOnly.processing, readOnly.nextBatch = readOnly.nextBatch, nil
	readOnly.round = r.heartbeatRound + 1
	r.bcastHeartbeat()
	log.Infof("%s has %d read-only requests get ready, next %d requests wait ack of round %d",
		r, len(rd), len(readOnly.processing), readOnly.round)
	return rd
}

func (r *Raft) MajorityAckRound() uint64 {
	var acks []uint64
	for _, p := range r.Prs {
		acks = append(acks, p.BeatAck)
	}
	return MajorityValue(acks)
}

func (r *Raft) isReadOnlyReady() bool {
	if r.State != StateLeader {
		return false
	}
	if t, _ := r.RaftLog.Term(r.RaftLog.committed); t != r.Term {
		return false
	}
	pending := len(r.readOnly.processing) + len(r.readOnly.nextBatch)
	return pending > 0 && r.MajorityAckRound() >= r.readOnly.round
}

type readOnly struct {
	round      uint64
	processing []*message.MsgRaftCmd
	nextBatch  []*message.MsgRaftCmd
}

func (ro *readOnly) String() string {
	return fmt.Sprintf("(waitRnd=%d,now=%d,next=%d)", ro.round, len(ro.processing), len(ro.nextBatch))
}
