package util

import (
	"math/big"
	"strings"
	"time"

	"github.com/kaiachain/kaia/blockchain/types"
	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/common/hexutil"
	"github.com/kaiachain/kaia/consensus/istanbul"
	"github.com/kaiachain/kaia/log"
	"github.com/kaiachain/kaia/params"
	ptypes "github.com/kaiachain/kaia/pipeline/types"
)

var (
	logger         = log.NewModuleLogger(log.Pipeline)
	istanbulSealer = istanbul.NewSealerImpl(nil)
)

// emptyUncleHash is the uncle hash Kaia reports over the eth namespace (Kaia has no uncles).
var emptyUncleHash = common.HexToHash("0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347")

// blockMiner returns the proposer of the block. Kaia's Rewardbase is not the
// proposer, so it is recovered from the Istanbul seal instead.
func blockMiner(header *types.Header) common.Address {
	miner, err := istanbulSealer.Author(header)
	if err != nil {
		if header.Number.Sign() != 0 {
			logger.Warn("Failed to ecrecover miner", "number", header.Number, "err", err)
		}
		return common.Address{}
	}
	return miner
}

func BuildPipelineBlock(rawBlock *types.Block) ptypes.Block {
	header := rawBlock.Header()
	block := ptypes.Block{
		ID:                    rawBlock.Hash().Hex(),
		Height:                rawBlock.Number(),
		ParentID:              rawBlock.ParentHash().Hex(),
		BaseFeePerGas:         big.NewInt(0),
		Miner:                 strings.ToLower(blockMiner(header).Hex()),
		GasLimit:              big.NewInt(int64(params.UpperGasLimit)),
		GasUsed:               big.NewInt(int64(rawBlock.GasUsed())),
		Timestamp:             header.Time.Uint64(),
		ProcessStartTimestamp: time.Now().UnixMilli(),
	}
	if header.BaseFee != nil {
		block.BaseFeePerGas = new(big.Int).Set(header.BaseFee)
	}
	return block
}

func BuildPilelineBlockHeader(block *types.Block) *ptypes.Header {
	header := block.Header()
	blockHeader := ptypes.Header{
		Number:     (*hexutil.Big)(block.Number()),
		Hash:       block.Hash(),
		ParentHash: block.ParentHash(),
		Nonce:      "0x0000000000000000",
		MixHash:    common.BytesToHash(header.MixHash),
		Sha3Uncles: emptyUncleHash,
		LogsBloom:  block.Bloom(),
		StateRoot:  block.Root(),
		Miner:      blockMiner(header),
		Difficulty: (*hexutil.Big)(header.BlockScore),
		// Same as api/api_eth.go: Kaia's extraData holds Istanbul consensus data
		// (validators, seals) that has no meaning to Ethereum consumers, so it is
		// reported as empty.
		ExtraData:        hexutil.Bytes{},
		GasLimit:         hexutil.Uint64(params.UpperGasLimit),
		GasUsed:          hexutil.Uint64(block.GasUsed()),
		Timestamp:        hexutil.Uint64(header.Time.Uint64()),
		TransactionsRoot: block.TxHash(),
		ReceiptsRoot:     block.ReceiptHash(),
	}
	if header.BaseFee != nil {
		blockHeader.BaseFeePerGas = (*hexutil.Big)(new(big.Int).Set(header.BaseFee))
	}
	if header.BlobGasUsed != nil {
		blockHeader.BlobGasUsed = (*hexutil.Uint64)(header.BlobGasUsed)
	}
	if header.ExcessBlobGas != nil {
		blockHeader.ExcessBlobGas = (*hexutil.Uint64)(header.ExcessBlobGas)
	}
	return &blockHeader
}

func BuildPipelineTransaction(tx *types.Transaction, receipt *types.Receipt, from common.Address, header *types.Header, config *params.ChainConfig) ptypes.Transaction {
	to := receipt.ContractAddress
	if tx.To() != nil {
		to = *tx.To()
	}
	gasPrice := tx.GasPrice()
	if config != nil {
		gasPrice = tx.EffectiveGasPrice(header, config)
	}
	transaction := ptypes.Transaction{
		ID:               tx.Hash().Hex(),
		From:             strings.ToLower(from.Hex()),
		To:               strings.ToLower(to.Hex()),
		Gas:              big.NewInt(int64(tx.Gas())),
		GasPrice:         gasPrice,
		GasUsed:          big.NewInt(int64(receipt.GasUsed)),
		Status:           receipt.Status == types.ReceiptStatusSuccessful,
		GasFeeCap:        common.Big0,
		GasTipCap:        common.Big0,
		Input:            tx.Data(),
		Nonce:            big.NewInt(int64(tx.Nonce())),
		TransactionIndex: int64(receipt.TranscationIndex),
		Value:            (*hexutil.Big)(tx.Value()),
	}
	// Only fee-market txs (EthereumDynamicFee, EthereumSetCode, ...) carry
	// their own fee cap / tip cap.
	if _, ok := tx.GetTxInternalData().(types.TxInternalDataBaseFee); ok {
		transaction.GasFeeCap = tx.GasFeeCap()
		transaction.GasTipCap = tx.GasTipCap()
	}
	return transaction
}
