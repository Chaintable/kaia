package api

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/kaiachain/kaia/accounts/abi"
	"github.com/kaiachain/kaia/blockchain"
	"github.com/kaiachain/kaia/blockchain/types"
	"github.com/kaiachain/kaia/blockchain/vm"
	"github.com/kaiachain/kaia/blockchain/vm/txtracev2"
	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/common/hexutil"
	"github.com/kaiachain/kaia/networks/rpc"
	"github.com/kaiachain/kaia/params"
)

type PreExecTx struct {
	ChainId              *big.Int        `json:"chainId,omitempty"`
	From                 *common.Address `json:"from"`
	To                   *common.Address `json:"to"`
	Gas                  *hexutil.Uint64 `json:"gas"`
	GasPrice             *hexutil.Big    `json:"gasPrice"`
	MaxFeePerGas         *hexutil.Big    `json:"maxFeePerGas"`
	MaxPriorityFeePerGas *hexutil.Big    `json:"maxPriorityFeePerGas"`
	Value                *hexutil.Big    `json:"value"`
	Nonce                *hexutil.Uint64 `json:"nonce"`
	Data                 *hexutil.Bytes  `json:"data"`
	Input                *hexutil.Bytes  `json:"input"`
}

// PreExecAPI provides pre exec info for rpc
type PreExecAPI struct {
	b Backend
}

func NewPreExecAPI(b Backend) *PreExecAPI {
	return &PreExecAPI{b: b}
}

const (
	UnKnown             = 1000
	InsufficientBalance = 1001
	Reverted            = 1002
)

type PreError struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

type PreResult struct {
	Trace     txtracev2.ActionTraceList `json:"trace"`
	Logs      []*types.Log              `json:"logs"`
	StateDiff txtracev2.StateDiff       `json:"stateDiff"`
	Error     PreError                  `json:"error,omitempty"`
	GasUsed   uint64                    `json:"gasUsed"`
}

func toPreError(err error, result *blockchain.ExecutionResult) PreError {
	preErr := PreError{
		Code: UnKnown,
	}
	if err != nil {
		preErr.Msg = err.Error()
	}
	if strings.HasPrefix(preErr.Msg, "evm: execution reverted") {
		preErr.Code = Reverted
		if result != nil {
			preErr.Msg, _ = abi.UnpackRevert(result.Return())
		}
	}
	if strings.HasPrefix(preErr.Msg, "out of gas") {
		preErr.Code = Reverted
	}
	if strings.HasPrefix(preErr.Msg, "insufficient funds") {
		preErr.Code = InsufficientBalance
	}
	if strings.HasPrefix(preErr.Msg, "insufficient balance") {
		preErr.Code = InsufficientBalance
	}
	return preErr
}

func (p *PreExecAPI) TraceMany(ctx context.Context, origins []PreExecTx) ([]PreResult, error) {
	preResList := make([]PreResult, 0)
	state, header, err := p.b.StateAndHeaderByNumberOrHash(ctx, rpc.NewBlockNumberOrHashWithNumber(rpc.LatestBlockNumber))
	if state == nil || err != nil {
		return nil, err
	}
	defer state.Close()
	for i := 0; i < len(origins); i++ {
		origin := origins[i]
		if origin.From == nil {
			origin.From = &common.Address{} // fake From address
		}
		if origin.Nonce == nil {
			preResList = append(preResList, PreResult{
				Error: PreError{
					Code: UnKnown,
					Msg:  "nonce is nil",
				},
			})
			continue
		}
		if i > 0 && (uint64)(*origin.Nonce) <= (uint64)(*origins[i-1].Nonce) {
			preResList = append(preResList, PreResult{
				Error: PreError{
					Code: UnKnown,
					Msg:  fmt.Sprintf("nonce decreases, tx index %d has nonce %d, tx index %d has nonce %d", i-1, (uint64)(*origins[i-1].Nonce), i, (uint64)(*origin.Nonce)),
				},
			})
			continue
		}
		txArgs := EthTransactionArgs{
			From:                 origins[i].From,
			To:                   origins[i].To,
			Gas:                  origins[i].Gas,
			GasPrice:             origins[i].GasPrice,
			MaxFeePerGas:         origins[i].MaxFeePerGas,
			MaxPriorityFeePerGas: origins[i].MaxPriorityFeePerGas,
			Value:                origins[i].Value,
			Data:                 origins[i].Data,
			Input:                origins[i].Input,
		}
		// header.BaseFee != nil means magma hardforked
		var baseFee *big.Int
		if header.BaseFee != nil {
			baseFee = header.BaseFee
		} else {
			baseFee = new(big.Int).SetUint64(params.ZeroBaseFee)
		}
		intrinsicGas, err := types.IntrinsicGas(txArgs.data(), nil, nil, txArgs.To == nil, p.b.ChainConfig().Rules(header.Number))
		if err != nil {
			if err != nil {
				preResList = append(preResList, PreResult{
					Error: PreError{
						Code: UnKnown,
						Msg:  err.Error(),
					},
				})
				continue
			}
		}
		gasCap := uint64(0)
		if rpcGasCap := p.b.RPCGasCap(); rpcGasCap != nil {
			gasCap = rpcGasCap.Uint64()
		}
		// Get a new instance of the EVM.
		msg, err := txArgs.ToMessage(gasCap, baseFee, intrinsicGas)
		if err != nil {
			preResList = append(preResList, PreResult{
				Error: PreError{
					Code: UnKnown,
					Msg:  err.Error(),
				},
			})
			continue
		}
		var balanceBaseFee *big.Int
		if header.BaseFee != nil {
			balanceBaseFee = new(big.Int).Mul(baseFee, common.Big2)
		} else {
			balanceBaseFee = msg.GasPrice()
		}
		// Add gas fee to sender for estimating gasLimit/computing cost or calling a function by insufficient balance sender.
		state.AddBalance(msg.ValidatedSender(), new(big.Int).Mul(new(big.Int).SetUint64(msg.Gas()), balanceBaseFee))
		if msg.Gas() < intrinsicGas {
			preResList = append(preResList, PreResult{
				Error: PreError{
					Code: UnKnown,
					Msg:  fmt.Sprintf("%v: msg.gas %d, want %d", blockchain.ErrIntrinsicGas, msg.Gas(), intrinsicGas),
				},
			})
			continue
		}

		tracer := txtracev2.NewOeTracer(nil, header.Hash(), header.Number, msg.Hash(), uint64(i))
		evm, _, err := p.b.GetEVM(ctx, msg, state, header, vm.Config{Tracer: tracer, Debug: true})
		if err != nil {
			preResList = append(preResList, PreResult{
				Error: PreError{
					Code: UnKnown,
					Msg:  err.Error(),
				},
			})
			continue
		}
		state.SetTxContext(msg.Hash(), header.Hash(), i)
		result, kerr := blockchain.ApplyMessage(evm, msg)
		if kerr != nil {
			preRes := PreResult{
				Error: toPreError(kerr, result),
			}
			if result != nil {
				preRes.GasUsed = result.UsedGas
			}
			preResList = append(preResList, preRes)
			continue
		}

		preRes := PreResult{
			Trace: tracer.GetTraces(),
			Logs:  state.GetLogs(msg.Hash()),
		}
		if result != nil {
			preRes.GasUsed = result.UsedGas
			if result.Failed() {
				preRes.Error = toPreError(err, result)
			}
		}

		if preRes.Error.Msg == "" && kerr != nil {
			preRes.Error = PreError{
				Code: Reverted,
				Msg:  err.Error(),
			}
		}
		preResList = append(preResList, preRes)
	}
	return preResList, nil
}
