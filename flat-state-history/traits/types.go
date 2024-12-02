package traits

import (
	"math/big"

	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/crypto"
)

// copy from nodex2.0

var EmptyCodeHash = crypto.Keccak256Hash(nil)

var EmptyRootHash = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

type NewAccount struct {
	Address  common.Hash
	Balance  *big.Int
	Nonce    uint64
	CodeHash common.Hash
	Root     common.Hash
}

type Account struct {
	Nonce    uint64
	Balance  *big.Int
	CodeHash common.Hash
	Root     common.Hash
}

type NewCode struct {
	CodeHash common.Hash
	Code     []byte
}

type IndexValuePair struct {
	Index common.Hash
	Value *big.Int
}

type AccountStorageDiff struct {
	Address common.Hash
	Values  []IndexValuePair
}

type BlockStorageDiff struct {
	Hash            common.Hash // for flatdb is empty, used for nodex2 rpc
	ParentHash      common.Hash // for flatdb is empty, used for nodex2 rpc
	NewAccounts     []NewAccount
	DeletedAccounts []common.Hash
	StorageDiff     []AccountStorageDiff
	NewCodes        []NewCode // for flatdb is empty, used for nodex2 statediff store
}
