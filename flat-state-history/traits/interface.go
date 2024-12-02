package traits

import (
	"github.com/kaiachain/kaia/common"
)

type HistoryWriter interface {
	// WriteStateDiff writes the given block storage diff to the history writer.
	WriteStateDiff(blockNumber uint64, diff *BlockStorageDiff) error
}

type FlatStateReader interface {
	// Root returns the block root for which this snapshot was made.
	BlockRoot() (common.Hash, error)
	// Account directly retrieves the account associated with a particular hash in
	// the snapshot slim data format.
	Account(hash common.Hash) (*Account, error)
	// Storage directly retrieves the storage data associated with a particular hash,
	// within a particular account.
	Storage(accountHash, storageHash common.Hash) (common.Hash, error)
	// Code directly retrieves the code associated with a particular hash in the snapshot
	// TODO: implement this for Nodex2.0
	Code(hash common.Hash) ([]byte, error)
	// Copy returns a copy of the StateReader
	Copy() FlatStateReader
	// Close closes the StateReader
	Close() error
}

type HistoryReader interface {
	// GetStateReader returns a StateReader for the given block number and block root hash.
	GetStateReader(blockNumber uint64) (FlatStateReader, common.Hash, error)
}
