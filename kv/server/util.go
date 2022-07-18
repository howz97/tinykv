package server

import (
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

func ErrKeyLocked(key []byte, lock *mvcc.Lock) *kvrpcpb.KeyError {
	return &kvrpcpb.KeyError{Locked: &kvrpcpb.LockInfo{
		PrimaryLock: lock.Primary,
		LockVersion: lock.Ts,
		Key:         key,
		LockTtl:     lock.Ttl,
	}}
}

func ErrConflict(start, conflict uint64, key, primary []byte) *kvrpcpb.KeyError {
	return &kvrpcpb.KeyError{
		Conflict: &kvrpcpb.WriteConflict{
			StartTs:    start,
			ConflictTs: conflict,
			Key:        key,
			Primary:    primary,
		},
	}
}

func ErrAbort(abort string) *kvrpcpb.KeyError {
	return &kvrpcpb.KeyError{
		Abort: abort,
	}
}

func CheckErr(err error) {
	if err != nil {
		panic(err)
	}
}
