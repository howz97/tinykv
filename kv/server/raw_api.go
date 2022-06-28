package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	rdr, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer rdr.Close()
	resp := &kvrpcpb.RawGetResponse{}
	val, err := rdr.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	resp.Value = val
	resp.NotFound = val == nil
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	mdf := storage.Modify{Data: storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}}
	err := server.storage.Write(req.Context, []storage.Modify{mdf})
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.RawPutResponse{}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	mdf := storage.Modify{Data: storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}}
	err := server.storage.Write(req.Context, []storage.Modify{mdf})
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.RawDeleteResponse{}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	resp := &kvrpcpb.RawScanResponse{}
	limit := req.Limit
	if limit <= 0 {
		return resp, nil
	}
	rdr, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer rdr.Close()
	iter := rdr.IterCF(req.Cf)
	defer iter.Close()
	iter.Seek(req.StartKey)
	for iter.Valid() {
		item := iter.Item()
		val, err := item.Value()
		if err != nil {
			continue
		}
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: val,
		})
		limit--
		if limit <= 0 {
			break
		}
		iter.Next()
	}
	return resp, nil
}
