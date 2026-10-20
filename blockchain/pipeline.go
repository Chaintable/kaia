// Copyright 2025 The Kaia Authors
// This file is part of the Kaia library.
//
// The Kaia library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The Kaia library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the Kaia library. If not, see <http://www.gnu.org/licenses/>.

package blockchain

import (
	"math/big"
	"slices"

	"github.com/kaiachain/kaia/blockchain/types"
	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/pipeline/leader"
	ptracer "github.com/kaiachain/kaia/pipeline/tracer"
	ptypes "github.com/kaiachain/kaia/pipeline/types"
)

// pipelineBlockEnd finishes the pipeline tracing of the block being inserted.
// A block that failed processing, validation or writing (blockErr != nil) is
// not uploaded.
func (bc *BlockChain) pipelineBlockEnd(blockErr error) {
	if bc.tracer != nil {
		bc.tracer.OnBlockEnd(blockErr)
	}
}

// pushPipelineBlockChange publishes a BlockChangeNotification for the new
// canonical head to Kafka. Every block from the last pushed block up to the
// head is announced, and blocks dropped by a reorg are listed as DropBlocks.
// It must be called after the head's data has been uploaded to S3.
func (bc *BlockChain) pushPipelineBlockChange(head *types.Block) {
	if bc.tracer == nil || leader.GlobalManager == nil || ptracer.NodeXPusher == nil {
		return
	}
	if !leader.GlobalManager.IsLeader() {
		return
	}
	leader.GlobalManager.RLock()
	lastPushedBlock := ptracer.NodeXPusher.LastPushedBlock()
	leader.GlobalManager.RUnlock()

	// Nothing was pushed yet (the genesis notice is pushed by OnGenesisBlock),
	// or the last pushed block is newer than the head: the chain was rewound,
	// and the change is pushed together with the next newer block.
	if lastPushedBlock == nil || lastPushedBlock.BlockNumber > head.NumberU64() {
		return
	}

	_, dropBlocks, newBlocks := bc.getCommonAncestor(*lastPushedBlock, toPipelineBlockContext(head.Header()))
	var blockChange *ptypes.BlockChangeNotification
	if len(dropBlocks) > 0 {
		blockChange = &ptypes.BlockChangeNotification{
			ChangeType: 2,
			NewBlocks:  newBlocks,
			DropBlocks: dropBlocks,
		}
	} else if len(newBlocks) > 0 {
		blockChange = &ptypes.BlockChangeNotification{
			ChangeType: 1,
			NewBlocks:  newBlocks,
		}
	}
	if blockChange == nil {
		return
	}
	if err := ptracer.NodeXPusher.PushBlockChangeNotification(blockChange, nil); err != nil {
		logger.Error("Failed to push pipeline block change notification", "number", head.NumberU64(), "hash", head.Hash(), "err", err)
		return
	}
	logger.Debug("Pushed pipeline block change notification", "changeType", blockChange.ChangeType,
		"newBlocks", len(blockChange.NewBlocks), "dropBlocks", len(blockChange.DropBlocks), "head", head.NumberU64())
}

// getCommonAncestor walks blocka and blockb back to their common ancestor. It
// returns the ancestor, the blocks of blocka's branch above it and the blocks
// of blockb's branch above it, both in ascending order.
func (bc *BlockChain) getCommonAncestor(blocka, blockb ptypes.BlockContext) (ptypes.BlockContext, []ptypes.BlockContext, []ptypes.BlockContext) {
	var chainA, chainB []ptypes.BlockContext
	if blockb.ParentHash == blocka.Hash {
		return blocka, chainA, []ptypes.BlockContext{blockb}
	}
	parentOf := func(b ptypes.BlockContext) ptypes.BlockContext {
		header := bc.GetHeaderByHash(b.ParentHash)
		if header == nil {
			logger.Crit("Failed to get header by hash", "hash", b.ParentHash)
		}
		return toPipelineBlockContext(header)
	}
	for blockb.BlockNumber > blocka.BlockNumber {
		chainB = append(chainB, blockb)
		blockb = parentOf(blockb)
	}
	for blocka.Hash != blockb.Hash {
		chainA = append(chainA, blocka)
		blocka = parentOf(blocka)
		chainB = append(chainB, blockb)
		blockb = parentOf(blockb)
	}
	// now blocka == blockb == ancestor
	slices.Reverse(chainA)
	slices.Reverse(chainB)
	return blocka, chainA, chainB
}

func toPipelineBlockContext(header *types.Header) ptypes.BlockContext {
	return ptypes.BlockContext{
		BlockNumber: header.Number.Uint64(),
		Hash:        header.Hash(),
		ParentHash:  header.ParentHash,
		Timestamp:   header.Time.Uint64(),
	}
}

// convertGenesisAllocToPipeline converts local GenesisAlloc to pipeline types GenesisAlloc
func convertGenesisAllocToPipeline(alloc GenesisAlloc) ptypes.GenesisAlloc {
	if alloc == nil {
		logger.Warn("Genesis alloc is nil")
		return nil
	}

	pipelineAlloc := make(ptypes.GenesisAlloc, len(alloc))
	for addr, account := range alloc {
		pipelineAccount := ptypes.GenesisAccount{
			Balance: new(big.Int).Set(account.Balance),
			Code:    common.CopyBytes(account.Code),
			Nonce:   account.Nonce,
		}
		if len(account.Storage) > 0 {
			pipelineAccount.Storage = make(map[common.Hash]common.Hash, len(account.Storage))
			for k, v := range account.Storage {
				pipelineAccount.Storage[k] = v
			}
		}
		pipelineAlloc[addr] = pipelineAccount
	}

	logger.Info("Converted genesis alloc to pipeline types", "allocCount", len(pipelineAlloc))
	return pipelineAlloc
}
