// Copyright 2021 The go-ethereum Authors
// // This file is part of the go-ethereum library.
// //
// // The go-ethereum library is free software: you can redistribute it and/or modify
// // it under the terms of the GNU Lesser General Public License as published by
// // the Free Software Foundation, either version 3 of the License, or
// // (at your option) any later version.
// //
// // The go-ethereum library is distributed in the hope that it will be useful,
// // but WITHOUT ANY WARRANTY; without even the implied warranty of
// // MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// // GNU Lesser General Public License for more details.
// //
// // You should have received a copy of the GNU Lesser General Public License
// // along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.
//
// // Package ethapi implements the general Ethereum API functions.

package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/kaiachain/kaia/blockchain/types"
	"github.com/kaiachain/kaia/blockchain/vm/txtracev2"
	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/common/hexutil"
	"github.com/kaiachain/kaia/rlp"
)

// PublicTxTraceAPI provides an API to tracing transaction or block information.
// // It offers only methods that operate on public data that is freely available to anyone.
type PublicTxTraceAPI struct {
	b Backend
}

// NewPublicTxTraceAPI creates a new trace API.
func NewPublicTxTraceAPI(b Backend) *PublicTxTraceAPI {
	return &PublicTxTraceAPI{b: b}
}

// Transaction trace_transaction function returns transaction traces.
func (api *PublicTxTraceAPI) Transaction(ctx context.Context, txHash common.Hash) (interface{}, error) {
	// Retrieve the transaction and assemble its EVM context
	tx, blockHash, blockNumber, index, receipt := api.b.GetTxLookupInfoAndReceipt(ctx, txHash)
	if tx == nil {
		return nil, fmt.Errorf("transaction %#x not found", txHash)
	}
	// It shouldn't happen in practice.
	if blockNumber == 0 {
		return nil, errors.New("genesis is not traceable")
	}

	if tx.IsValueTransfer() {
		actionType := "call"
		from := getFrom(tx)
		value := hexutil.Big(*tx.Value())
		addressAction := txtracev2.Action{
			CallType: &actionType,
			From:     &from,
			To:       tx.To(),
			Value:    &value,
			Gas:      hexutil.Uint64(tx.Gas()),
		}
		action := txtracev2.ActionTrace{
			Action:              addressAction,
			BlockHash:           blockHash,
			BlockNumber:         big.NewInt(int64(blockNumber)),
			TraceAddress:        []uint32{},
			TransactionHash:     txHash,
			TransactionPosition: uint64(index),
			TraceType:           "call",
		}
		traceResult := txtracev2.ActionResult{
			GasUsed: hexutil.Uint64(receipt.GasUsed),
		}

		action.Result = &traceResult
		if receipt.Status != types.ReceiptStatusSuccessful {
			action.Error = "Reverted"
		}

		actionlist := txtracev2.ActionTraceList{action}

		return actionlist, nil
	}

	flatten := new(txtracev2.ActionTraceList)
	store := api.b.GetTxTraceStore()
	if store == nil {
		return []byte{}, fmt.Errorf("transaction trace unavailable on non-trace node")
	}

	raw, err := store.ReadTxTrace(context.Background(), txHash)
	if err != nil {
		return []byte{}, err
	}
	if bytes.Equal(raw, []byte{}) { // empty response
		return nil, fmt.Errorf("trace result of tx {%#v} not found in tracedb", txHash)
	}

	err = rlp.DecodeBytes(raw, flatten)
	if err != nil {
		return nil, fmt.Errorf("failed to decode rlp flatten traces: %v", err)
	}

	return *flatten, nil
}
