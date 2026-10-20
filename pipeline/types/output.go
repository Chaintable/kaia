package types

import "github.com/kaiachain/kaia/common/hexutil"

type DebankOutPut struct {
	BlockFile      *BlockFile    `json:"block_file"`
	Header         *Header       `json:"header"`
	StateDiff      hexutil.Bytes `json:"state_diff"`
	ValidationHash int64         `json:"validation_hash"`
}
