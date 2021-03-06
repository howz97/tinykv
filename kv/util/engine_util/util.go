package engine_util

import (
	"bytes"
	"fmt"

	"github.com/Connor1996/badger"
	"github.com/golang/protobuf/proto"
)

func KeyWithCF(cf string, key []byte) []byte {
	return append([]byte(cf+"_"), key...)
}

func GetCF(db *badger.DB, cf string, key []byte) (val []byte, err error) {
	err = db.View(func(txn *badger.Txn) error {
		val, err = GetCFFromTxn(txn, cf, key)
		return err
	})
	return
}

func GetCFFromTxn(txn *badger.Txn, cf string, key []byte) (val []byte, err error) {
	item, err := txn.Get(KeyWithCF(cf, key))
	if err != nil {
		return nil, err
	}
	val, err = item.ValueCopy(val)
	return
}

func PutCF(engine *badger.DB, cf string, key []byte, val []byte) error {
	return engine.Update(func(txn *badger.Txn) error {
		return txn.Set(KeyWithCF(cf, key), val)
	})
}

func GetMeta(engine *badger.DB, key []byte, msg proto.Message) error {
	var val []byte
	err := engine.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.Value()
		return err
	})
	if err != nil {
		return err
	}
	return proto.Unmarshal(val, msg)
}

func GetMetaFromTxn(txn *badger.Txn, key []byte, msg proto.Message) error {
	item, err := txn.Get(key)
	if err != nil {
		return err
	}
	val, err := item.Value()
	if err != nil {
		return err
	}
	return proto.Unmarshal(val, msg)
}

func PutMeta(engine *badger.DB, key []byte, msg proto.Message) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return engine.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
}

func DeleteCF(engine *badger.DB, cf string, key []byte) error {
	return engine.Update(func(txn *badger.Txn) error {
		return txn.Delete(KeyWithCF(cf, key))
	})
}

func DeleteRange(db *badger.DB, startKey, endKey []byte) error {
	batch := new(WriteBatch)
	txn := db.NewTransaction(false)
	defer txn.Discard()
	for _, cf := range CFs {
		deleteRangeCF(txn, batch, cf, startKey, endKey)
	}

	return batch.WriteToDB(db)
}

func deleteRangeCF(txn *badger.Txn, batch *WriteBatch, cf string, startKey, endKey []byte) {
	it := NewCFIterator(cf, txn)
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)
		if ExceedEndKey(key, endKey) {
			break
		}
		batch.DeleteCF(cf, key)
	}
	defer it.Close()
}

func GetRange(db *badger.DB, start, end []byte) string {
	txn := db.NewTransaction(false)
	defer txn.Discard()
	kvs := "{"
	for _, cf := range CFs {
		s := getRangeCF(txn, cf, start, end)
		if len(s) == 0 {
			continue
		}
		kvs += cf + "@"
		kvs += fmt.Sprint(s)
	}
	kvs += "}"
	return kvs
}

func getRangeCF(txn *badger.Txn, cf string, startKey, endKey []byte) (kvs []string) {
	it := NewCFIterator(cf, txn)
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)
		if ExceedEndKey(key, endKey) {
			break
		}
		val := ""
		v, err := item.Value()
		if err != nil {
			val = err.Error()
		} else {
			val = string(v)
		}
		kvs = append(kvs, fmt.Sprintf("%s:%s", string(key), val))
	}
	it.Close()
	return
}

func ExceedEndKey(current, endKey []byte) bool {
	if len(endKey) == 0 {
		return false
	}
	return bytes.Compare(current, endKey) >= 0
}
