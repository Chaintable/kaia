package api

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/kaiachain/kaia/accounts/abi"
	"github.com/kaiachain/kaia/blockchain"
	"github.com/kaiachain/kaia/blockchain/state"
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
	for i := 0; i < len(origins); i++ {
		stateOld := state.Copy()
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
		intrinsicGas, err := types.IntrinsicGas(txArgs.data(), nil, txArgs.To == nil, p.b.ChainConfig().Rules(header.Number))
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
		execResult, kerr := blockchain.ApplyMessage(evm, msg)
		if kerr != nil {
			preRes := PreResult{
				Error:   toPreError(kerr, execResult),
				GasUsed: execResult.UsedGas,
			}
			preResList = append(preResList, preRes)
			continue
		}
		preRes := PreResult{
			Trace:   tracer.GetTraces(),
			Logs:    state.GetLogs(msg.Hash()),
			GasUsed: execResult.UsedGas,
		}
		if preRes.Error.Msg == "" && preRes.Trace != nil && len(preRes.Trace) > 0 && (preRes.Trace)[0].Error != "" {
			preRes.Error = PreError{
				Code: Reverted,
				Msg:  (preRes.Trace)[0].Error,
			}
		}
		if preRes.Error.Code == 0 {
			gasCap := uint64(0)
			if rpcGasCap := p.b.RPCGasCap(); rpcGasCap != nil {
				gasCap = rpcGasCap.Uint64()
			}
			usedGas := p.doEstimateGas(ctx, txArgs, header, stateOld, gasCap)
			if usedGas > 0 {
				preRes.GasUsed = usedGas
			}
		}
		preResList = append(preResList, preRes)
	}
	return preResList, nil
}

func (p *PreExecAPI) doEstimateGas(ctx context.Context, args EthTransactionArgs, header *types.Header, state *state.StateDB, gasCap uint64) uint64 {
	// Binary search the gas requirement, as it may be higher than the amount used
	var (
		lo  uint64 = params.TxGas - 1
		hi  uint64 = params.UpperGasLimit
		cap uint64
	)
	// Use zero address if sender unspecified.
	if args.From == nil {
		args.From = new(common.Address)
	}
	// Determine the highest gas limit can be used during the estimation.
	if args.Gas != nil && uint64(*args.Gas) >= params.TxGas {
		hi = uint64(*args.Gas)
	} else {
		// Ethereum set hi as gas ceiling of the block but,
		// there is no actual gas limit in Klaytn, so we set it as params.UpperGasLimit.
		hi = params.UpperGasLimit
	}
	// Normalize the max fee per gas the call is willing to spend.
	var feeCap *big.Int
	if args.GasPrice != nil && (args.MaxFeePerGas != nil || args.MaxPriorityFeePerGas != nil) {
		return 0
	} else if args.GasPrice != nil {
		feeCap = args.GasPrice.ToInt()
	} else if args.MaxFeePerGas != nil {
		feeCap = args.MaxFeePerGas.ToInt()
	} else {
		feeCap = common.Big0
	}
	if feeCap.BitLen() != 0 {
		balance := state.GetBalance(*args.From) // from can't be nil
		available := new(big.Int).Set(balance)
		if args.Value != nil {
			if args.Value.ToInt().Cmp(available) >= 0 {
				return 0
			}
			available.Sub(available, args.Value.ToInt())
		}
		allowance := new(big.Int).Div(available, feeCap)

		// If the allowance is larger than maximum uint64, skip checking
		if allowance.IsUint64() && hi > allowance.Uint64() {
			transfer := args.Value
			if transfer == nil {
				transfer = new(hexutil.Big)
			}
			hi = allowance.Uint64()
		}
	}
	// Recap the highest gas allowance with specified gascap.
	if gasCap != 0 && hi > gasCap {
		hi = gasCap
	}
	cap = hi
	executable := func(gas uint64) (bool, *blockchain.ExecutionResult, error) {
		args.Gas = (*hexutil.Uint64)(&gas)
		result, err := p.doCallCopy(ctx, args, header, state, gasCap)
		if err != nil {
			if errors.Is(err, blockchain.ErrIntrinsicGas) {
				// Special case, raise gas limit
				return false, nil, nil
			}
			// Returns error when it is not VM error (less balance or wrong nonce, etc...).
			return true, nil, err
		}
		// If err is vmError, return vmError with returned data
		return result.Failed(), result, nil
	}
	// Execute the binary search and hone in on an executable gas limit
	for lo+1 < hi {
		mid := (hi + lo) / 2
		isExecutable, _, err := executable(mid)
		if err != nil {
			return 0
		}

		if !isExecutable {
			lo = mid
		} else {
			hi = mid
		}
	}
	// Reject the transaction as invalid if it still fails at the highest allowance
	if hi == cap {
		isExecutable, result, err := executable(hi)
		if err != nil {
			return 0
		}
		if !isExecutable {
			if result != nil && result.VmExecutionStatus != types.ReceiptStatusErrOutOfGas {
				return 0
			}
		}
	}
	return hi
}

func (p *PreExecAPI) doCallCopy(ctx context.Context, args EthTransactionArgs, header *types.Header, state *state.StateDB, globalGasCap uint64) (*blockchain.ExecutionResult, error) {
	stateNew := state.Copy()
	// header.BaseFee != nil means magma hardforked
	var baseFee *big.Int
	if header.BaseFee != nil {
		baseFee = header.BaseFee
	} else {
		baseFee = new(big.Int).SetUint64(params.ZeroBaseFee)
	}
	intrinsicGas, err := types.IntrinsicGas(args.data(), nil, args.To == nil, p.b.ChainConfig().Rules(header.Number))
	if err != nil {
		return nil, err
	}
	msg, err := args.ToMessage(globalGasCap, baseFee, intrinsicGas)
	if err != nil {
		return nil, err
	}
	var balanceBaseFee *big.Int
	if header.BaseFee != nil {
		balanceBaseFee = new(big.Int).Mul(baseFee, common.Big2)
	} else {
		balanceBaseFee = msg.GasPrice()
	}
	// Add gas fee to sender for estimating gasLimit/computing cost or calling a function by insufficient balance sender.
	stateNew.AddBalance(msg.ValidatedSender(), new(big.Int).Mul(new(big.Int).SetUint64(msg.Gas()), balanceBaseFee))

	// The intrinsicGas is checked again later in the blockchain.ApplyMessage function,
	// but we check in advance here in order to keep StateTransition.TransactionDb method as unchanged as possible
	// and to clarify error reason correctly to serve eth namespace APIs.
	// This case is handled by EthDoEstimateGas function.
	if msg.Gas() < intrinsicGas {
		return nil, fmt.Errorf("%w: msg.gas %d, want %d", blockchain.ErrIntrinsicGas, msg.Gas(), intrinsicGas)
	}
	evm, vmError, err := p.b.GetEVM(ctx, msg, stateNew, header, vm.Config{})
	if err != nil {
		return nil, err
	}
	if err := vmError(); err != nil {
		return nil, err
	}

	// Execute the message.
	res, err := blockchain.ApplyMessage(evm, msg)
	if err != nil {
		return res, fmt.Errorf("err: %w (supplied gas %d)", err, msg.Gas())
	}
	return res, nil
}
