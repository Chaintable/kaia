// Copyright 2021 The klaytn Authors
// This file is part of the klaytn library.
//
// The klaytn library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The klaytn library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the klaytn library. If not, see <http://www.gnu.org/licenses/>.

package api

import (
	"context"
	"fmt"
	"math/big"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/kaiachain/kaia/accounts/abi"
	"github.com/kaiachain/kaia/blockchain"
	"github.com/kaiachain/kaia/blockchain/types"
	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/common/hexutil"
	"github.com/kaiachain/kaia/networks/rpc"
	"github.com/kaiachain/kaia/params"
)

type MultiCallResp struct {
	Results []*callResult   `json:"results"`
	Stats   *multiCallStats `json:"stats"`
}

type callResult struct {
	Code      int           `json:"code"`
	Err       string        `json:"err"`
	FromCache bool          `json:"fromCache"`
	Result    hexutil.Bytes `json:"result"`
	GasUsed   int64         `json:"gasUsed"`
	TimeCost  float64       `json:"timeCost"`
}

type multiCallStats struct {
	BlockNum     int64       `json:"blockNum"`
	BlockHash    common.Hash `json:"blockHash"`
	BlockTime    int64       `json:"blockTime"`
	Success      bool        `json:"success"`
	CacheEnabled bool        `json:"cacheEnabled"`
}

const (
	singleCallTimeout = 5 * time.Second
	multiCallLimit    = 50

	// client param error
	errCodeTxArgs               = -40000
	errNativeMethodNotFound     = -40001
	errNativeMethodInput        = -40002
	errNativeMethodInputAddress = -40003
	errNativeExecuting          = -40004
	errBlockNotFound            = -40005

	// evm processing error
	errNativeMethodOutput     = -40010
	errNativeMethodStateError = -40011
	errMessageExecuting       = -40012
	errEVMCancelled           = -40013
	errEVMReverted            = -40014
	errEVMFastFailed          = -40015

	// internal error
	errUnderlyingDB = -40020
	errLoadingState = -40021
)

const (
	nativeAddr = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
)

var (
	// copied from: accounts/abi/abi_test.go
	Uint8, _   = abi.NewType("uint8", "", nil)
	Uint256, _ = abi.NewType("uint256", "", nil)
	String, _  = abi.NewType("string", "", nil)
	Address, _ = abi.NewType("address", "", nil)

	erc20ABI = abi.ABI{
		Methods: map[string]abi.Method{
			"name":        funcName,
			"symbol":      funcSymbol,
			"decimals":    funcDecimals,
			"totalSupply": funcTotalSupply,
			"balanceOf":   funcBalanceOf,
		},
	}

	funcName = abi.NewMethod("name", "name", abi.Function, "", false, false,
		[]abi.Argument{},
		[]abi.Argument{
			{Name: "", Type: String, Indexed: false},
		},
	)
	funcSymbol = abi.NewMethod("symbol", "symbol", abi.Function, "", false, false,
		[]abi.Argument{},
		[]abi.Argument{
			{Name: "", Type: String, Indexed: false},
		},
	)
	funcDecimals = abi.NewMethod("decimals", "decimals", abi.Function, "", false, false,
		[]abi.Argument{},
		[]abi.Argument{
			{Name: "", Type: Uint8, Indexed: false},
		},
	)
	funcTotalSupply = abi.NewMethod("totalSupply", "totalSupply", abi.Function, "", false, false,
		[]abi.Argument{},
		[]abi.Argument{
			{Name: "", Type: Uint256, Indexed: false},
		},
	)
	funcBalanceOf = abi.NewMethod("balanceOf", "balanceOf", abi.Function, "", false, false,
		[]abi.Argument{
			{Name: "", Type: Address, Indexed: false},
		},
		[]abi.Argument{
			{Name: "", Type: Uint256, Indexed: false},
		},
	)
)

func handleNative(ctx context.Context, b Backend, arg EthTransactionArgs, blockNrOrHash rpc.BlockNumberOrHash, overrides *EthStateOverride, globalGasCap uint64) ([]byte, error) {
	state, header, err := b.StateAndHeaderByNumberOrHash(ctx, blockNrOrHash)
	if state == nil || err != nil {
		return nil, err
	}
	defer state.Close()
	if err := overrides.Apply(state); err != nil {
		return nil, err
	}

	// header.BaseFee != nil means magma hardforked
	var baseFee *big.Int
	if header.BaseFee != nil {
		baseFee = header.BaseFee
	} else {
		baseFee = new(big.Int).SetUint64(params.ZeroBaseFee)
	}
	intrinsicGas, err := types.IntrinsicGas(arg.data(), nil, nil, arg.To == nil, b.ChainConfig().Rules(header.Number))
	if err != nil {
		return nil, err
	}
	msg, err := arg.ToMessage(globalGasCap, baseFee, intrinsicGas)
	if err != nil {
		return nil, err
	}

	data := msg.Data()
	method, err := erc20ABI.MethodById(data)
	if err != nil {
		return nil, err
	}
	switch method.Name {
	case "name", "symbol":
		res, err := method.Outputs.Pack("KLAY")
		if err != nil {
			return nil, err
		}
		return res, nil
	case "decimals":
		res, err := method.Outputs.Pack(uint8(18))
		if err != nil {
			return nil, err
		}
		return res, nil
	case "totalSupply":
		res, err := method.Outputs.Pack(big.NewInt(1_000_000_000_000_000_000)) // 1 ETH
		if err != nil {
			return nil, err
		}
		return res, nil
	case "balanceOf":
		inputs, err := method.Inputs.Unpack(data[4:])
		if err != nil || len(inputs) == 0 {
			return nil, err
		}
		address, ok := inputs[0].(common.Address)
		if !ok {
			return nil, fmt.Errorf("input address error")
		}
		balance, err := method.Outputs.Pack(state.GetBalance(common.Address(address)))
		if err != nil {
			return nil, err
		}
		if state.Error() != nil {
			return nil, state.Error()
		}
		return balance, nil
	default:
		return nil, fmt.Errorf("method not found")
	}
}

func doOneCall(ctx context.Context, b Backend, arg EthTransactionArgs, header *types.Header, blockNrOrHash rpc.BlockNumberOrHash, overrides *EthStateOverride, timeout time.Duration, globalGasCap uint64) (*callResult, error) {
	var err error
	var result = &callResult{}

	start := time.Now()

	// make sure this will be called prior to the SetCallCache defer func on returning
	defer func() {
		result.TimeCost = time.Since(start).Seconds()
	}()

	// skip EVM if requests for native token
	if strings.ToLower(arg.To.Hex()) == nativeAddr {
		res, err := handleNative(ctx, b, arg, blockNrOrHash, overrides, globalGasCap)
		if err != nil {
			result.Code = errNativeExecuting
			result.Err = err.Error()
		}
		result.Result = res
		return result, err
	}

	execResult, err := EthDoCall(ctx, b, arg, blockNrOrHash, overrides, timeout, globalGasCap)
	if err != nil {
		result.Code = errMessageExecuting
		result.Err = err.Error()
		return result, err
	}

	if len(execResult.Revert()) > 0 {
		err2 := blockchain.NewRevertError(execResult)
		result.Code = errMessageExecuting
		result.Err = err2.Error()
		return result, err2
	}

	result.Result = execResult.Return()
	result.GasUsed = int64(execResult.UsedGas)
	return result, nil
}

func (s *EthereumAPI) MultiCall(ctx context.Context, args []EthTransactionArgs, blockNrOrHash rpc.BlockNumberOrHash, pfastFail, puseParallel, pdisableCache *bool, overrides *EthStateOverride) (resp *MultiCallResp, err error) {

	// maximum calls check
	if len(args) > multiCallLimit {
		return nil, fmt.Errorf("calls exceed limit, expected: <%v, actual: %v", multiCallLimit, len(args))
	}

	setb := func(p *bool, d bool) bool {
		if p == nil {
			return d
		}
		return *p
	}

	fastFail := setb(pfastFail, true)
	useParallel := setb(puseParallel, true)
	disableCache := setb(pdisableCache, false)

	bcAPI := s.publicBlockChainAPI.b
	gasCap := uint64(0)
	if rpcGasCap := bcAPI.RPCGasCap(); rpcGasCap != nil {
		gasCap = rpcGasCap.Uint64()
	}

	// check block & state
	state, header, err := bcAPI.StateAndHeaderByNumberOrHash(ctx, blockNrOrHash)
	if state == nil || err != nil {
		return nil, err
	}
	defer state.Close()

	blockTime := header.Time
	ret := make([]*callResult, len(args))
	stats := &multiCallStats{
		BlockNum:     header.Number.Int64(),
		BlockHash:    header.Hash(),
		BlockTime:    blockTime.Int64(),
		Success:      true,
		CacheEnabled: !disableCache,
	}

	ctx, cancel := context.WithTimeout(ctx, singleCallTimeout)
	defer cancel()

	if useParallel {
		// run in parallel
		var wg sync.WaitGroup
		for i, arg := range args {
			wg.Add(1)
			go func(i int, arg EthTransactionArgs) {
				defer func() {
					if r := recover(); r != nil {
						fmt.Println("stacktrace from panic: \n" + string(debug.Stack()))
					}
				}()
				defer wg.Done()

				r, _ := doOneCall(ctx, bcAPI, arg, header, blockNrOrHash, overrides, bcAPI.RPCEVMTimeout(), gasCap)
				ret[i] = r
				if r.Err != "" {
					stats.Success = false
					if fastFail {
						cancel()
					}
					return
				}
			}(i, arg)
		}
		wg.Wait()

		return &MultiCallResp{Results: ret, Stats: stats}, nil
	}

	// run in sequence
	failedOnce := false
	for i, arg := range args {
		if failedOnce {
			ret[i] = &callResult{}
			continue
		}

		r, _ := doOneCall(ctx, bcAPI, arg, header, blockNrOrHash, overrides, bcAPI.RPCEVMTimeout(), gasCap)
		ret[i] = r
		if r.Err != "" {
			stats.Success = false
			if fastFail {
				failedOnce = true
			}
			continue
		}
	}

	return &MultiCallResp{Results: ret, Stats: stats}, nil
}
