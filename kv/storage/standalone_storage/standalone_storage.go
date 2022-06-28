package standalone_storage

import (
	"os"

	"github.com/Connor1996/badger"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	conf *config.Config
	db   *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		conf: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	path := s.conf.DBPath
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return err
	}
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	s.db, err = badger.Open(opts)
	return err
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &Reader{
		txn: s.db.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		for _, m := range batch {
			key := m.Key()
			val := m.Value()
			cf := m.Cf()
			var err error
			if val == nil {
				err = engine_util.DeleteCF(s.db, cf, key)
			} else {
				err = engine_util.PutCF(s.db, cf, key, val)
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

type Reader struct {
	txn *badger.Txn
}

func (r *Reader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (r *Reader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *Reader) Close() {
	r.txn.Discard()
}
