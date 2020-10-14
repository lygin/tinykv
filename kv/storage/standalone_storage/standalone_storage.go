package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	Db     *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{Db:engine_util.CreateDB(conf.DBPath, conf.Raft)}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	if err := s.Db.Close(); err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &StandAloneStorageReader{txn:s.Db.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	write_batch := new(engine_util.WriteBatch)
	for _, modify := range batch {
		switch v := modify.Data.(type) {
		case storage.Put:
			write_batch.SetCF(v.Cf, v.Key, v.Value)
		case storage.Delete:
			write_batch.DeleteCF(v.Cf, v.Key)
		}
	}
	return write_batch.WriteToDB(s.Db)
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (sr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(sr.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (sr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.txn)
}

func (sr *StandAloneStorageReader) Close() {
	sr.txn.Discard()
}
