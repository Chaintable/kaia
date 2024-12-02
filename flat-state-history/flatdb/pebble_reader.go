package flatdb

import (
	"bytes"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/flat-state-history/traits"
	"github.com/kaiachain/kaia/rlp"
)

var _ traits.FlatStateReader = (*PebbleStateReader)(nil)

const MaxIterAliveTime = 15 * time.Second

type PebbleStateReader struct {
	iter        *pebble.Iterator
	blockNumber uint64
	db          *pebble.DB
	mu          sync.Mutex
	closed      bool
}

func NewPebbleStateReader(db *pebble.DB, blockNumber uint64) (*PebbleStateReader, common.Hash, error) {
	iter, err := db.NewIter(nil)
	if err != nil {
		return nil, common.Hash{}, err
	}
	r := &PebbleStateReader{db: db, iter: iter, blockNumber: blockNumber}
	blockRoot, err := r.BlockRoot()
	if err != nil {
		iter.Close()
		return nil, common.Hash{}, err
	}
	go func(r *PebbleStateReader) {
		<-time.After(MaxIterAliveTime)
		r.Close()
	}(r)
	return r, blockRoot, nil

}

func (r *PebbleStateReader) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	return r.iter.Close()
}

func (r *PebbleStateReader) BlockRoot() (common.Hash, error) {
	key := append(BlockRootPrefix, EncodeBlockNumber(r.blockNumber)...)
	r.iter.SeekGE(key)
	if !r.iter.Valid() {
		return common.Hash{}, traits.ErrBlockNotFound
	}
	if !bytes.Equal(r.iter.Key()[:len(BlockRootPrefix)], BlockRootPrefix) {
		return common.Hash{}, traits.ErrBlockNotFound
	}
	return common.BytesToHash(r.iter.Value()), nil
}

func (r *PebbleStateReader) AccountRlp(accountKey []byte) ([]byte, error) {
	acckey := append(AccountPrefix, accountKey...)
	accNumkey := append(acckey, EncodeBlockNumber(r.blockNumber+1)...)
	r.iter.SeekLT(accNumkey)
	if !r.iter.Valid() {
		return nil, nil
	}
	if !bytes.Equal(r.iter.Key()[:len(acckey)], acckey) {
		return nil, nil
	}
	return r.iter.Value(), nil
}

func (r *PebbleStateReader) Account(hash common.Hash) (*traits.Account, error) {
	value, err := r.AccountRlp(hash.Bytes())
	if err != nil {
		return nil, err
	}
	if len(value) == 0 {
		return nil, nil
	}
	slimAccount := new(SlimAccount)
	if err := rlp.DecodeBytes(value, slimAccount); err != nil {
		panic(err)
	}
	if len(slimAccount.CodeHash) == 0 {
		slimAccount.CodeHash = traits.EmptyCodeHash
	}
	return &traits.Account{
		Balance:  slimAccount.Balance,
		Nonce:    slimAccount.Nonce,
		CodeHash: slimAccount.CodeHash,
		Root:     slimAccount.Root,
	}, nil
}

func (r *PebbleStateReader) Storage(accountHash, storageHash common.Hash) (common.Hash, error) {
	storageKey := append(StoragePrefix, accountHash.Bytes()...)
	storageKey = append(storageKey, storageHash.Bytes()...)
	storageNumKey := append(storageKey, EncodeBlockNumber(r.blockNumber+1)...)
	r.iter.SeekLT(storageNumKey)
	if !r.iter.Valid() {
		return common.Hash{}, nil
	}
	if !bytes.Equal(r.iter.Key()[:len(storageKey)], storageKey) {
		return common.Hash{}, nil
	}
	blockNumer := DecodeBlockNumber(r.iter.Key()[len(storageKey):])
	if blockNumer > r.blockNumber {
		return common.Hash{}, nil
	}
	return common.BytesToHash(r.iter.Value()), nil
}

// nodex2.0 need it
func (r *PebbleStateReader) Code(hash common.Hash) ([]byte, error) {
	panic("not implemented")
}

func (r *PebbleStateReader) Copy() traits.FlatStateReader {
	reader, _, _ := NewPebbleStateReader(r.db, r.blockNumber)
	return reader
}
