package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pkg/errors"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	DB *badger.DB
}

type Reader struct {
	Txn *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	return &StandAloneStorage{
		DB: db,
	}

	return nil
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.DB.NewTransaction(true)
	reader := new(Reader)
	reader.Txn = txn
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, m := range batch {
		key := m.Key()
		value := m.Value()
		cf := m.Cf()
		switch m.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.DB, cf, key, value)
			if err != nil {
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.DB, cf, key)
			if err != nil {
				return err
			}
		default:
			return errors.New("Unexpected modify type")
		}
	}

	return nil
}

func (r *Reader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.Txn, cf, key)
	defer r.Txn.Discard()
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return val, err
}

func (r *Reader) IterCF(cf string) engine_util.DBIterator {
	iter := engine_util.NewCFIterator(cf, r.Txn)
	defer r.Txn.Discard()
	defer iter.Close()
	return iter
}

func (r *Reader) Close() {

}
