package flatdb

import (
	"bytes"
	"math"

	"github.com/cockroachdb/pebble"
	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/flat-state-history/traits"
	"github.com/kaiachain/kaia/rlp"
)

var _ traits.HistoryReader = (*PebbleHistoryReader)(nil)

type PebbleHistoryReader struct {
	db *pebble.DB
}

func GetReader() *PebbleHistoryReader {
	if store == nil {
		return nil
	}
	return NewPebbleHistoryReader(store)
}

func NewPebbleHistoryReader(db *pebble.DB) *PebbleHistoryReader {
	return &PebbleHistoryReader{db: db}
}

func (r *PebbleHistoryReader) GetStateReader(blockNumber uint64) (traits.FlatStateReader, common.Hash, error) {
	return NewPebbleStateReader(r.db, blockNumber)
}

func (r *PebbleHistoryReader) GetLatestStateNumber() (uint64, common.Hash, error) {
	iter, err := r.db.NewIter(nil)
	if err != nil {
		return 0, common.Hash{}, err
	}
	defer iter.Close()
	key := append(BlockRootPrefix, EncodeBlockNumber(math.MaxUint64)...)
	iter.SeekLT(key)
	if !iter.Valid() {
		logger.Info("GetLatestStateNumber", "seekLT", false)
		return 0, common.Hash{}, nil
	}
	if !bytes.Equal(iter.Key()[:len(BlockRootPrefix)], BlockRootPrefix) {
		logger.Info("GetLatestStateNumber", "seekLT", false, "key", iter.Key())
		return 0, common.Hash{}, traits.ErrBlockNotFound
	}
	blockNumber := DecodeBlockNumber(iter.Key()[len(BlockRootPrefix):])
	return blockNumber, common.BytesToHash(iter.Value()), nil
}

func (r *PebbleHistoryReader) GetBlockDiffAfter(blockNumber uint64) (map[uint64][]*traits.BlockStorageDiff, error) {
	iter, err := r.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	mapDiff := make(map[uint64][]*traits.BlockStorageDiff)
	firstKey := append(DiffPrefix, EncodeBlockNumber(blockNumber+1)...)
	if !iter.SeekGE(firstKey) {
		logger.Error("GetBlockDiff", "blockNumber", blockNumber, "firstKey", firstKey, "seekGE", false)
		return mapDiff, nil
	}
	for iter.Valid() {
		key := iter.Key()
		if !bytes.Equal(key[:len(DiffPrefix)], DiffPrefix) {
			break
		}
		blockNumber := DecodeBlockNumber(key[len(DiffPrefix) : len(DiffPrefix)+8])
		value := iter.Value()
		blockStorageDiff := &traits.BlockStorageDiff{}
		err := rlp.DecodeBytes(value, blockStorageDiff)
		if err != nil {
			return nil, err
		}
		mapDiff[blockNumber] = append(mapDiff[blockNumber], blockStorageDiff)
		iter.Next()
	}
	return mapDiff, nil
}

func (r *PebbleHistoryReader) GetBlockDiff(blockNumber uint64, blockRoot common.Hash) (*traits.BlockStorageDiff, error) {
	key := append(DiffPrefix, EncodeBlockNumber(blockNumber)...)
	key = append(key, blockRoot.Bytes()...)
	value, closer, err := r.db.Get(key)
	defer closer.Close()
	if err != nil {
		return nil, err
	}
	if len(value) == 0 {
		return nil, traits.ErrBlockNotFound
	}
	blockStorageDiff := &traits.BlockStorageDiff{}
	err = rlp.DecodeBytes(value, blockStorageDiff)
	if err != nil {
		return nil, err
	}
	return blockStorageDiff, nil
}
