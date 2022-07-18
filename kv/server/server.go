package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
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
	if err != nil {
		return nil, err
	}
	if lock != nil {
		if lock.Ts < txn.StartTS {
			resp.Error = ErrKeyLocked(req.Key, lock)
			return resp, nil
		}
	}
	val, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	resp.Value = val
	if len(val) == 0 {
		resp.NotFound = true
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
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
	resp := new(kvrpcpb.CommitResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		w, _, err := txn.CurrentWrite(key)
		CheckErr(err)
		if w != nil {
			if w.Kind == mvcc.WriteKindRollback {
				resp.Error = ErrAbort("already aborted")
				break
			}
			continue
		}
		lock, err := txn.GetLock(key)
		CheckErr(err)
		if lock == nil {
			resp.Error = ErrAbort("not locked")
			break
		}
		if lock.Ts != txn.StartTS {
			resp.Error = ErrAbort("other locked")
			break
		}
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindPut,
		})
		txn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
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
