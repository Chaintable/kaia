package flatdb

import (
	"encoding/binary"
	"math/big"

	"github.com/kaiachain/kaia/common"
)

var (
	// AccountPrefix+AccountKey+BlockNumber -> AccountValue
	AccountPrefix = []byte("a")
	// StoragePrefix+AccountKey+StorageKey+BlockNumber -> StorageValue
	StoragePrefix = []byte("s")
	// BlockRootPrefix+BlockNumber -> BlockRoot
	BlockRootPrefix = []byte("b")
	// DiffPrefix+BlockNumber+BlockRoot -> Diff
	DiffPrefix = []byte("d")
)

func EncodeBlockNumber(blockNumber uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, blockNumber)
	return b
}

func DecodeBlockNumber(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

type SlimAccount struct {
	Balance  *big.Int
	Nonce    uint64
	CodeHash common.Hash
	Root     common.Hash
}
