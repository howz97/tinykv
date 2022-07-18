package mvcc

import (
	"github.com/pingcap-incubator/tinykv/kv/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	nextKey []byte
	txn     *MvccTxn
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	return &Scanner{
		nextKey: startKey,
		txn:     txn,
	}
}

func (scan *Scanner) Close() {
	scan.nextKey = nil
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	var (
		key []byte
		val []byte
		err error
	)
	for len(val) == 0 {
		if scan.nextKey == nil {
			break
		}
		key = scan.nextKey
		var lock *Lock
		lock, err = scan.txn.GetLock(key)
		util.CheckErr(err)
		if lock != nil && lock.Ts > scan.txn.StartTS {
			err = ErrKeyLocked(key, lock)
			scan.updateNextKey()
			break
		}
		val, err = scan.txn.GetValue(key)
		util.CheckErr(err)
		scan.updateNextKey()
	}
	return key, val, err
}

func (scan *Scanner) updateNextKey() {
	iter := scan.txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()
	iter.Seek(EncodeKey(scan.nextKey, 0))
	if !iter.Valid() {
		scan.nextKey = nil
		return
	}
	scan.nextKey = DecodeUserKey(iter.Item().Key())
}
