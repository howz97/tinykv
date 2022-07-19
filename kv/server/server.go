package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	resp := new(kvrpcpb.GetResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	CheckErr(err)
	if lock != nil {
		if lock.Ts < txn.StartTS {
			resp.Error = ErrKeyLocked(req.Key, lock)
			return resp, nil
		}
	}
	val, err := txn.GetValue(req.Key)
	CheckErr(err)
	resp.Value = val
	resp.NotFound = val == nil
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	var keys [][]byte
	for _, mut := range req.Mutations {
		keys = append(keys, mut.Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	resp := new(kvrpcpb.PrewriteResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, mut := range req.Mutations {
		_, ts, err := txn.MostRecentWrite(mut.Key)
		CheckErr(err)
		if ts > txn.StartTS {
			resp.Errors = append(resp.Errors, ErrConflict(txn.StartTS, ts, mut.Key, req.PrimaryLock))
			continue
		}
		lock, err := txn.GetLock(mut.Key)
		CheckErr(err)
		if lock != nil {
			resp.Errors = append(resp.Errors, ErrKeyLocked(mut.Key, lock))
			continue
		}
		txn.PutLock(mut.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindFromProto(mut.Op),
		})
		txn.PutValue(mut.Key, mut.Value)
	}
	if len(resp.Errors) > 0 {
		return resp, nil
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	resp := new(kvrpcpb.CommitResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	if kerr := commit(txn, req.CommitVersion, req.Keys); kerr != nil {
		resp.Error = kerr
		return resp, nil
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
	}
	return resp, nil
}

func commit(txn *mvcc.MvccTxn, ver uint64, keys [][]byte) *kvrpcpb.KeyError {
	for _, key := range keys {
		w, _, err := txn.CurrentWrite(key)
		CheckErr(err)
		if w != nil {
			if w.Kind == mvcc.WriteKindRollback {
				return ErrAbort("already aborted")
			}
			continue
		}
		lock, err := txn.GetLock(key)
		CheckErr(err)
		if lock == nil {
			return ErrAbort("not locked")
		}
		if lock.Ts != txn.StartTS {
			return ErrAbort("other locked")
		}
		txn.PutWrite(key, ver, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindPut,
		})
		txn.DeleteLock(key)
	}
	return nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	resp := new(kvrpcpb.ScanResponse)
	if req.Limit == 0 {
		return resp, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	for k, v, e := scanner.Next(); v != nil || e != nil; k, v, e = scanner.Next() {
		if v != nil {
			resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{Key: k, Value: v})
		} else {
			resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{Error: &e.(*mvcc.KeyError).KeyError})
		}
		if uint32(len(resp.Pairs)) >= req.Limit {
			break
		}
	}
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	server.Latches.WaitForLatches([][]byte{req.PrimaryKey})
	defer server.Latches.ReleaseLatches([][]byte{req.PrimaryKey})

	resp := new(kvrpcpb.CheckTxnStatusResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	w, ts, err := txn.CurrentWrite(req.PrimaryKey)
	CheckErr(err)
	if w != nil {
		if w.Kind != mvcc.WriteKindRollback {
			resp.CommitVersion = ts
		}
		return resp, nil
	}
	lock, err := txn.GetLock(req.PrimaryKey)
	CheckErr(err)
	if lock != nil {
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl < mvcc.PhysicalTime(req.CurrentTs) {
			txn.PutWrite(lock.Primary, lock.Ts, &mvcc.Write{StartTS: lock.Ts, Kind: mvcc.WriteKindRollback})
			txn.DeleteValue(req.PrimaryKey)
			txn.DeleteLock(lock.Primary)
			resp.Action = kvrpcpb.Action_TTLExpireRollback
		} else {
			resp.LockTtl = lock.Ttl
		}
	} else {
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{StartTS: req.LockTs, Kind: mvcc.WriteKindRollback})
		resp.Action = kvrpcpb.Action_LockNotExistRollback
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	resp := new(kvrpcpb.BatchRollbackResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	if kerr := rollback(txn, req.StartVersion, req.Keys); kerr != nil {
		resp.Error = kerr
		return resp, nil
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
	}
	return resp, nil
}

func rollback(txn *mvcc.MvccTxn, ver uint64, keys [][]byte) *kvrpcpb.KeyError {
	for _, key := range keys {
		w, _, err := txn.CurrentWrite(key)
		CheckErr(err)
		if w != nil {
			if w.Kind != mvcc.WriteKindRollback {
				return ErrAbort("can not rollback, already commited")
			}
			continue
		}
		txn.PutWrite(key, ver, &mvcc.Write{
			StartTS: ver,
			Kind:    mvcc.WriteKindRollback,
		})
		txn.DeleteValue(key)
		lock, err := txn.GetLock(key)
		CheckErr(err)
		if lock != nil && lock.Ts == ver {
			txn.DeleteLock(key)
		}
	}
	return nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	resp := new(kvrpcpb.ResolveLockResponse)
	keys, re := server.txnKeysLocked(req.Context, req.StartVersion)
	if re != nil {
		resp.RegionError = re
		return resp, nil
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	if req.CommitVersion > 0 {
		resp.Error = commit(txn, req.CommitVersion, keys)
	} else {
		resp.Error = rollback(txn, req.StartVersion, keys)
	}
	if resp.Error == nil {
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			resp.RegionError = util.RaftstoreErrToPbError(err)
		}
	}
	return resp, nil
}

func (server *Server) txnKeysLocked(ctx *kvrpcpb.Context, ver uint64) ([][]byte, *errorpb.Error) {
	reader, err := server.storage.Reader(ctx)
	if err != nil {
		return nil, util.RaftstoreErrToPbError(err)
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, ver)
	var keys [][]byte
	iter := txn.Reader.IterCF(engine_util.CfLock)
	defer iter.Close()
	for iter.Seek([]byte("")); iter.Valid(); iter.Next() {
		item := iter.Item()
		lock, err := txn.GetLock(item.Key())
		CheckErr(err)
		if lock.Ts == ver {
			keys = append(keys, item.KeyCopy(nil))
		}
	}
	return keys, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
