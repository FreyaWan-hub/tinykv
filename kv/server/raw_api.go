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
	// Your Code Here (1).
	db := server.storage
	reader, err := db.Reader(nil)
	if err != nil {
		return nil, err
	}
	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return nil, err
	}

	resp := &kvrpcpb.RawGetResponse{}
	if value == nil {
		resp.NotFound = true
	}
	resp.Value = value

	return resp, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}
	modify := storage.Modify{Data: put}

	db := server.storage
	err := db.Write(nil, []storage.Modify{modify})
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.RawPutResponse{}

	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	delete := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}
	modify := storage.Modify{Data: delete}

	db := server.storage
	err := db.Write(nil, []storage.Modify{modify})
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.RawDeleteResponse{}

	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	db := server.storage
	reader, err := db.Reader(nil)
	if err != nil {
		return nil, err
	}

	iter := reader.IterCF(req.GetCf())

	startKey := req.GetStartKey()
	resp := &kvrpcpb.RawScanResponse{
		Kvs: make([]*kvrpcpb.KvPair, 0),
	}
	var nums uint32
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		if nums >= req.GetLimit() {
			break
		}
		item := iter.Item()
		key := item.Key()
		value, err := item.Value()
		if err != nil {
			return resp, err
		}
		kvPair := &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		}
		resp.Kvs = append(resp.Kvs, kvPair)

		nums++
	}

	return resp, nil
}
