package types

import (
	"math/big"

	"github.com/kaiachain/kaia/common"
)

type GenesisAlloc map[common.Address]GenesisAccount

type GenesisAccount struct {
	Code    []byte                      `json:"code,omitempty"`
	Storage map[common.Hash]common.Hash `json:"storage,omitempty"`
	Balance *big.Int                    `json:"balance" gencodec:"required"`
	Nonce   uint64                      `json:"nonce,omitempty"`
}
