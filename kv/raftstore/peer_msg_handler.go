package raftstore

import (
	"fmt"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	if !d.peer.RaftGroup.HasReady() {
		return
	}
	rd := d.peer.RaftGroup.Ready()
	result, err := d.peer.peerStorage.SaveReadyState(&rd)
	if err != nil {
		log.Panicf("SaveReadyState %v", err)
		return
	}
	if result != nil && !util.RegionEqual(result.PrevRegion, result.Region) {
		// update storeMeta
		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()
		storeMeta.regions[d.regionId] = result.Region
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: result.Region})
		storeMeta.Unlock()
		kvs := engine_util.GetRange(d.ctx.engine.Kv, result.Region.StartKey, result.Region.EndKey)
		log.Infof("region state changed from %s to %s: kvs=%v", result.PrevRegion, result.Region, kvs)
	}
	d.peer.Send(d.ctx.trans, rd.Messages)
	for _, ent := range rd.CommittedEntries {
		if len(ent.Data) == 0 {
			continue
		}
		cb := d.takeProposal(ent.Term, ent.Index)
		var resp *raft_cmdpb.RaftCmdResponse
		switch ent.EntryType {
		case eraftpb.EntryType_EntryNormal:
			resp = d.applyNormal(ent)
			if cb != nil && len(resp.Responses) > 0 && resp.Responses[0].CmdType == raft_cmdpb.CmdType_Snap {
				log.Infof("peer %s applied and response snap entry=(t%d,i%d)", d.Tag, ent.Term, ent.Index)
				cb.Txn = d.ctx.engine.Kv.NewTransaction(false)
			}
		case eraftpb.EntryType_EntryConfChange:
			resp = d.applyConfChange(ent)
		}
		cb.Done(resp)
		if d.stopped {
			return
		}
	}
	for _, cmd := range rd.ReadOnly {
		req := cmd.Request.Requests[0]
		resp := newCmdResp()
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			r, err := d.processGet(req.Get)
			if err != nil {
				resp = ErrResp(err)
				break
			}
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Get,
				Get:     r,
			})
		case raft_cmdpb.CmdType_Snap:
			r, err := d.processSnap(cmd.Request)
			if err != nil {
				resp = ErrResp(err)
				break
			}
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Snap,
				Snap:    r,
			})
			cmd.Callback.Txn = d.ctx.engine.Kv.NewTransaction(false)
		}
		cmd.Callback.Done(resp)
		if cmd.Request.Header.Serial > d.ClientSerial[cmd.Request.Header.Client] {
			d.ClientSerial[cmd.Request.Header.Client] = cmd.Request.Header.Serial
		}
	}
	d.RaftGroup.Advance(rd)
}

func (d *peerMsgHandler) applyNormal(ent eraftpb.Entry) *raft_cmdpb.RaftCmdResponse {
	reqs := new(raft_cmdpb.RaftCmdRequest)
	err := reqs.Unmarshal(ent.Data)
	util.CheckErr(err)
	resp, err := d.process(reqs)
	if err != nil {
		return ErrResp(err)
	}
	return resp
}

func (d *peerMsgHandler) applyConfChange(ent eraftpb.Entry) (resp *raft_cmdpb.RaftCmdResponse) {
	resp = newCmdResp()
	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
		ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: new(metapb.Region)},
	}
	cc := new(eraftpb.ConfChange)
	err := cc.Unmarshal(ent.Data)
	util.CheckErr(err)
	peer := new(metapb.Peer)
	err = peer.Unmarshal(cc.Context)
	util.CheckErr(err)
	region := d.peerStorage.region
	if ignoreChangePeer(region, cc.ChangeType, peer.StoreId) {
		log.Warnf("applyConfChange %v peer %s but already excuted", cc.ChangeType, peer.String())
		err := util.CloneMsg(region, resp.AdminResponse.ChangePeer.Region)
		util.CheckErr(err)
		return resp
	}
	log.Infof("peer %s applyConfChange(t%d,i%d) region %d: %s %d .", d.RaftGroup.Raft.String(), ent.Term, ent.Index, region.Id, cc.ChangeType, cc.NodeId)
	region.RegionEpoch.ConfVer++
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		region.Peers = append(region.Peers, peer)
	case eraftpb.ConfChangeType_RemoveNode:
		util.RemovePeer(region, peer.StoreId)
		d.removePeerCache(peer.Id)
		if d.PeerId() == peer.Id && d.MaybeDestroy() {
			// todo:
			d.destroyPeer()
			err = util.CloneMsg(region, resp.AdminResponse.ChangePeer.Region)
			util.CheckErr(err)
			return
		}
	default:
		panic("unknown")
	}
	log.Infof("peer %s applyConfChange(t%d,i%d), newest config %s", d.RaftGroup.Raft.String(), ent.Term, ent.Index, region)
	engine_util.PutMeta(d.peerStorage.Engines.Kv, meta.RegionStateKey(d.regionId), &rspb.RegionLocalState{Region: region})
	d.RaftGroup.ApplyConfChange(*cc)
	err = util.CloneMsg(region, resp.AdminResponse.ChangePeer.Region)
	util.CheckErr(err)
	return
}

func (d *peerMsgHandler) process(reqs *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.RaftCmdResponse, error) {
	resp := newCmdResp()
	if reqs.AdminRequest != nil {
		return d.processAdmin(reqs)
	}
	for _, req := range reqs.Requests {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Put:
			if reqs.Header.Serial > d.peer.ClientSerial[reqs.Header.Client] {
				d.peer.ClientSerial[reqs.Header.Client] = reqs.Header.Serial
			}
			r, err := d.processPut(req.Put)
			if err != nil {
				return nil, err
			}
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Put,
				Put:     r,
			})
			log.Infof("%s applied put command %v", d.Tag, req)
		case raft_cmdpb.CmdType_Delete:
			if reqs.Header.Serial > d.peer.ClientSerial[reqs.Header.Client] {
				d.peer.ClientSerial[reqs.Header.Client] = reqs.Header.Serial
			}
			r, err := d.processDel(req.Delete)
			if err != nil {
				return nil, err
			}
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete:  r,
			})
			log.Infof("%s applied delete command %v", d.Tag, req)
		default:
			panic("unknown CmdType")
		}
	}
	return resp, nil
}

func (d *peerMsgHandler) processSnap(reqs *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.SnapResponse, error) {
	if reqs.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
		err := &util.ErrEpochNotMatch{
			Message: "region split occured",
			Regions: []*metapb.Region{util.CopyRegion(d.Region())},
		}
		return nil, err
	}
	resp := new(raft_cmdpb.SnapResponse)
	resp.Region = util.CopyRegion(d.Region())
	return resp, nil
}

func (d *peerMsgHandler) processGet(req *raft_cmdpb.GetRequest) (*raft_cmdpb.GetResponse, error) {
	if err := util.CheckKeyInRegion(req.Key, d.Region()); err != nil {
		log.Infof("processGet CheckKeyInRegion %v", err)
		return nil, err
	}
	val, err := engine_util.GetCF(d.ctx.engine.Kv, req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	resp := new(raft_cmdpb.GetResponse)
	resp.Value = val
	return resp, nil
}

func (d *peerMsgHandler) processPut(req *raft_cmdpb.PutRequest) (*raft_cmdpb.PutResponse, error) {
	err := util.CheckKeyInRegion(req.Key, d.Region())
	if err != nil {
		return nil, err
	}
	err = engine_util.PutCF(d.ctx.engine.Kv, req.Cf, req.Key, req.Value)
	util.CheckErr(err)
	resp := new(raft_cmdpb.PutResponse)
	d.SizeDiffHint += uint64(len(req.Key) + len(req.Value))
	return resp, nil
}

func (d *peerMsgHandler) processDel(req *raft_cmdpb.DeleteRequest) (*raft_cmdpb.DeleteResponse, error) {
	resp := new(raft_cmdpb.DeleteResponse)
	if err := util.CheckKeyInRegion(req.Key, d.Region()); err != nil {
		return resp, err
	}
	err := engine_util.DeleteCF(d.ctx.engine.Kv, req.Cf, req.Key)
	util.CheckErr(err)
	return resp, nil
}

func (d *peerMsgHandler) processAdmin(reqs *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.RaftCmdResponse, error) {
	resp := &raft_cmdpb.AdminResponse{
		CmdType: reqs.AdminRequest.CmdType,
	}
	switch reqs.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		req := reqs.AdminRequest.CompactLog
		state := d.peerStorage.applyState
		if req.CompactIndex <= state.TruncatedState.Index {
			log.Warnf("%s ignore invalid compact %d<=%d", d.Tag, req.CompactIndex, state.TruncatedState.Index)
			break
		}
		state.TruncatedState.Index = req.CompactIndex
		state.TruncatedState.Term = req.CompactTerm
		err := engine_util.PutMeta(d.peerStorage.Engines.Kv, meta.ApplyStateKey(d.regionId), state)
		util.CheckErr(err)
		d.ScheduleCompactLog(req.CompactIndex)
		resp.CompactLog = new(raft_cmdpb.CompactLogResponse)
	case raft_cmdpb.AdminCmdType_Split:
		if util.IsEpochStale(reqs.Header.RegionEpoch, d.Region().RegionEpoch) {
			log.Warnf("%s failed to split because epoch is stale, %s but current is %s", d.Tag, reqs.Header.RegionEpoch, d.Region().RegionEpoch)
			// re-scan region size
			d.ApproximateSize = nil
			return nil, &util.ErrEpochNotMatch{Message: "AdminCmdType_Split region epoch not match"}
		}
		resp.Split = d.processAdminSplit(reqs.AdminRequest.Split)
	}
	return &raft_cmdpb.RaftCmdResponse{Header: new(raft_cmdpb.RaftResponseHeader), AdminResponse: resp}, nil
}

func (d *peerMsgHandler) processAdminSplit(req *raft_cmdpb.SplitRequest) *raft_cmdpb.SplitResponse {
	log.Infof("%s processAdminSplit req %v", d.Tag, req)
	if len(req.NewPeerIds) == 0 {
		log.Fatalf("processAdminSplit split request is invalid: %s", req)
		return nil
	}
	region0 := d.Region()
	if util.CheckKeyInRegion(req.SplitKey, region0) != nil {
		log.Warnf("processAdminSplit key not in region %s req=%s, maybe already splited", d.Region(), req)
		return nil
	}
	storeMeta := d.ctx.storeMeta
	storeMeta.Lock()
	defer storeMeta.Unlock()
	if _, ok := storeMeta.regions[req.NewRegionId]; ok {
		log.Fatalf("%s processAdminSplit but new region already exist, maybe replicatePeer created it", d.Tag)
		return nil
	}
	// change meta data
	region1 := &metapb.Region{
		Id:       req.NewRegionId,
		StartKey: req.SplitKey,
		EndKey:   region0.EndKey,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: region0.RegionEpoch.ConfVer,
			Version: region0.RegionEpoch.Version + 1,
		},
	}
	for i, id := range req.NewPeerIds {
		if i >= len(region0.Peers) {
			log.Warnf("%s config change occured during region split, current region=%s, req.NewPeerIds=%v",
				d.Tag, region0, req.NewPeerIds)
			break
		}
		region1.Peers = append(region1.Peers, &metapb.Peer{
			Id:      id,
			StoreId: region0.Peers[i].StoreId,
		})
	}
	region0.RegionEpoch.Version++
	region0.EndKey = req.SplitKey
	engine_util.PutMeta(d.ctx.engine.Kv, meta.RegionStateKey(region0.Id), &rspb.RegionLocalState{Region: region0})
	engine_util.PutMeta(d.ctx.engine.Kv, meta.RegionStateKey(region1.Id), &rspb.RegionLocalState{Region: region1})
	storeMeta.regions[region0.Id] = region0
	storeMeta.regions[region1.Id] = region1
	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region0})
	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region1})

	// start peer of region1
	peer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, region1)
	util.CheckErr(err)
	d.ctx.router.register(peer)
	err = d.ctx.router.send(region1.Id, message.Msg{Type: message.MsgTypeStart})
	util.CheckErr(err)

	// response
	resp := new(raft_cmdpb.SplitResponse)
	resp.Regions = append(resp.Regions, util.CopyRegion(region0), util.CopyRegion(region1))
	log.Infof("peer %s on store %v finished to split region, new peer=%v, regions=%v ### %v",
		d.Tag, d.storeID(), peer.Tag, region0, region1)
	if d.IsLeader() {
		// let a random peer to tell PD that new region has been generated
		peer.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	if len(storeMeta.pendingVotes) > 0 {
		// region split finished, send pending votes if needed
		pending := make([]*rspb.RaftMessage, 0, len(storeMeta.pendingVotes))
		for _, msg := range storeMeta.pendingVotes {
			if msg.RegionId == region1.Id {
				d.ctx.router.send(msg.RegionId, message.NewPeerMsg(message.MsgTypeRaftMessage, msg.RegionId, msg))
			} else {
				pending = append(pending, msg)
			}
		}
		storeMeta.pendingVotes = pending
	}
	return resp
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, string(split.SplitKey))
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(raftCmd *message.MsgRaftCmd) {
	msg := raftCmd.Request
	cb := raftCmd.Callback
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	if msg.AdminRequest != nil {
		admReq := msg.AdminRequest
		switch admReq.CmdType {
		case raft_cmdpb.AdminCmdType_ChangePeer:
			d.proposalChangerPeer(admReq.ChangePeer, cb)
			return
		case raft_cmdpb.AdminCmdType_TransferLeader:
			d.proposalTransfer(admReq.TransferLeader, cb)
			return
		}
	} else {
		// optimize:
		// response directly if this retry has already executed
		if resp, txn := d.ReplyRetryInstantly(msg); resp != nil {
			cb.Txn = txn
			cb.Done(resp)
			log.Infof("%s ResponseRetry skip %v", d.Tag, msg)
			return
		}
		if util.ReadOnlyCmd[msg.Requests[0].CmdType] {
			d.peer.RaftGroup.ProposalRead(raftCmd)
			log.Infof("%s proposaled read only request %s", d.Tag, msg.String())
			return
		}
	}

	data, err := msg.Marshal()
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	err = d.RaftGroup.Propose(data)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// record CallBack
	lt, li := d.RaftGroup.Raft.RaftLog.LastEntry()
	if cb != nil {
		d.recordProposal(lt, li, cb)
	}
	log.Infof("%s proposed raft entry (t%d,i%d), %s", d.Tag, lt, li, msg)
}

func (d *peerMsgHandler) ReplyRetryInstantly(msg *raft_cmdpb.RaftCmdRequest) (resp *raft_cmdpb.RaftCmdResponse, txn *badger.Txn) {
	if msg.Header == nil || msg.Header.Serial == 0 || len(msg.Requests) != 1 {
		return
	}
	if msg.Header.Serial > d.ClientSerial[msg.Header.Client] {
		return
	}
	req := msg.Requests[0]
	resp = newCmdResp()
	resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
		CmdType: req.CmdType,
	})
	switch req.CmdType {
	case raft_cmdpb.CmdType_Put:
		resp.Responses[0].Put = new(raft_cmdpb.PutResponse)
	case raft_cmdpb.CmdType_Delete:
		resp.Responses[0].Delete = new(raft_cmdpb.DeleteResponse)
	case raft_cmdpb.CmdType_Get:
		r, err := d.processGet(req.Get)
		if err != nil {
			resp = ErrResp(err)
			break
		}
		resp.Responses[0].Get = r
	case raft_cmdpb.CmdType_Snap:
		r, err := d.processSnap(msg)
		if err != nil {
			resp = ErrResp(err)
			break
		}
		resp.Responses[0].Snap = r
		txn = d.ctx.engine.Kv.NewTransaction(false)
	default:
		panic("never execute")
	}
	return
}

func (d *peerMsgHandler) proposalTransfer(req *raft_cmdpb.TransferLeaderRequest, cb *message.Callback) {
	err := d.RaftGroup.TransferLeader(req.Peer.Id)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	resp := newCmdResp()
	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
		TransferLeader: new(raft_cmdpb.TransferLeaderResponse),
	}
	cb.Done(resp)
}

func (d *peerMsgHandler) proposalChangerPeer(req *raft_cmdpb.ChangePeerRequest, cb *message.Callback) {
	// ignore duplicate config change command caused by re-try
	if ignoreChangePeer(d.peerStorage.region, req.ChangeType, req.Peer.StoreId) {
		log.Warnf("proposalChangerPeer %v peer %s but already excuted", req.ChangeType, req.Peer.String())
		resp := newCmdResp()
		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: new(metapb.Region)},
		}
		err := util.CloneMsg(d.Region(), resp.AdminResponse.ChangePeer.Region)
		util.CheckErr(err)
		cb.Done(resp)
		return
	}
	if req.ChangeType == eraftpb.ConfChangeType_RemoveNode && req.Peer.Id == d.PeerId() {
		ee := d.RaftGroup.Raft.ChooseTransferee()
		if ee == raft.None {
			panic("can not transfer leader")
		}
		err := d.RaftGroup.TransferLeader(ee)
		log.Warnf("leader %s proposal remove self, transfer leader to peer%v first: %v", d.Tag, ee, err)
		return
	}

	log.Infof("proposalChangerPeer %s %s", req.ChangeType, req.Peer.String())
	ctx, err := req.Peer.Marshal()
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	err = d.RaftGroup.ProposeConfChange(eraftpb.ConfChange{
		ChangeType: req.ChangeType,
		NodeId:     req.Peer.Id,
		Context:    ctx,
	})
	if err != nil {
		log.Errorf("proposalChangerPeer failed %v", err)
		cb.Done(ErrResp(err))
		return
	}
	lt, li := d.RaftGroup.Raft.RaftLog.LastEntry()
	d.recordProposal(lt, li, cb)
}

func ignoreChangePeer(region *metapb.Region, typ eraftpb.ConfChangeType, store uint64) bool {
	switch typ {
	case eraftpb.ConfChangeType_AddNode:
		if util.FindPeer(region, store) != nil {
			return true
		}
	case eraftpb.ConfChangeType_RemoveNode:
		if util.FindPeer(region, store) == nil {
			return true
		}
	default:
		return true
	}
	return false
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.peerStorage.truncatedIndex() + 1,
		EndIdx:     truncatedIndex + 1,
	}
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
	log.Infof("ScheduleCompactLog region=%d, range=[%d, %d)",
		raftLogGCTask.RegionID, raftLogGCTask.StartIdx, raftLogGCTask.EndIdx)
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg.Message.GetMsgType(), from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}
	// avoid frequently compaction
	if d.peerStorage.AppliedIndex() < d.LastCompactedIdx {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	log.Infof("onRaftGCLogTick propose admin command to make snapshot, region=%d, (t%d,i%d)",
		regionID, term, compactIdx)
	d.proposeRaftCommand(&message.MsgRaftCmd{Request: request})
	d.LastCompactedIdx = d.RaftGroup.Raft.RaftLog.LastIndex()
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}
	if !d.IsLeader() {
		// reset ApproximateSize to check split after become leader again
		d.ApproximateSize = nil
		d.SizeDiffHint = 0
		return
	}
	approx := uint64(d.ctx.cfg.RegionMaxSize)
	if d.ApproximateSize != nil {
		approx = *d.ApproximateSize
	}
	if approx+d.SizeDiffHint < d.ctx.cfg.RegionMaxSize {
		log.Debugf("onSplitRegionCheckTick %s do not send msg: %v, (Approx=%v+hint=%v) splitMax=%d/%d",
			d.Tag, d.ApproximateSize, approx, d.SizeDiffHint, d.ctx.cfg.RegionMaxSize, d.ctx.cfg.RegionSplitSize)
		return
	}
	log.Infof("onSplitRegionCheckTick %s send check split message: (Approx=%v + hint=%v) splitMax=%d/%d",
		d.Tag, approx, d.SizeDiffHint, d.ctx.cfg.RegionMaxSize, d.ctx.cfg.RegionSplitSize)
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	// SplitCheckTask maybe time consuming, set smaller ApproximateSize to avoid duplicated scan
	// ApproximateSize will be replaced by scan result as soon as scan finished
	d.ApproximateSize = &d.ctx.cfg.RegionSplitSize
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
	log.Infof("%s onApproximateRegionSize(%d)", d.Tag, size)
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
