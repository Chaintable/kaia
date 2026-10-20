package tracer

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kaiachain/kaia/blockchain/types"
	"github.com/kaiachain/kaia/blockchain/vm"
	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/common/hexutil"
	"github.com/kaiachain/kaia/params"
	"github.com/kaiachain/kaia/pipeline/leader"
	"github.com/kaiachain/kaia/pipeline/metrics"
	ptypes "github.com/kaiachain/kaia/pipeline/types"
	"github.com/kaiachain/kaia/pipeline/util"
)

// 需要上传3种data
// 1. block
// 2. state diff
// 3. block file

var _ vm.Tracer = (*PipelineTracer)(nil)

type PipelineTracer struct {
	config      pipelineTracerConfig
	callTracer  *callTracer
	chainConfig *params.ChainConfig
}

type pipelineTracerConfig struct {
	Region               string   `json:"region"`
	NodeXBucket          string   `json:"node_x_bucket"`
	ChainTableBucket     string   `json:"chain_table_bucket"`
	Brokers              []string `json:"brokers"`
	Topic                string   `json:"topic"`
	S3TempDir            string   `json:"s3_temp_dir"`
	IsBackup             *bool    `json:"is_backup"`              // nil = auto (use etcd), false = leader in fixed mode, true = backup in fixed mode
	EnablePreStateTracer bool     `json:"enable_prestate_tracer"` // not supported on Kaia, the state diff always comes from statedb commit
	Version              string   `json:"version"`

	// Auto failover configurations
	EtcdEndpoints []string `json:"etcd_endpoints"`
	ElectionKey   string   `json:"election_key"`
	NodeID        string   `json:"node_id"`      // default to hostname
	GracePeriod   int      `json:"grace_period"` // default to 10 seconds, unit is second

	// Writer node registry configurations
	WriterRegistryTTL int64 `json:"writer_registry_ttl"` // TTL for writer node registration in seconds, default 30
}

func (config *pipelineTracerConfig) fillDefaultValues() {
	if config.IsBackup == nil && len(config.EtcdEndpoints) == 0 {
		logger.Crit("IsBackup is nil and etcd endpoints is empty, please set IsBackup to true(manual mode) or add etcd endpoints(auto mode)")
	}
	if config.IsBackup != nil && len(config.EtcdEndpoints) > 0 {
		logger.Crit("IsBackup is not nil and etcd endpoints is not empty, please set IsBackup to nil(auto mode) or remove etcd endpoints(manual mode)")
	}
	if config.EnablePreStateTracer {
		logger.Warn("enable_prestate_tracer is not supported on Kaia, ignoring it")
		config.EnablePreStateTracer = false
	}
	if config.NodeID == "" {
		// 先尝试从环境变量 HOSTNAME 读取
		if hostname := os.Getenv("HOSTNAME"); hostname != "" {
			config.NodeID = hostname
		} else {
			// 如果环境变量不存在，则使用系统主机名
			hostname, err := os.Hostname()
			if err != nil {
				logger.Crit("Failed to get hostname", "err", err)
			}
			config.NodeID = hostname
		}
	}
	if config.GracePeriod == 0 {
		config.GracePeriod = 10
	}
	// Fill default values for writer registry
	if config.WriterRegistryTTL == 0 {
		config.WriterRegistryTTL = 10 // 10 seconds default TTL
	}
}

func NewPipelineTracer(cfg json.RawMessage) (*PipelineTracer, error) {
	var config pipelineTracerConfig
	if cfg != nil {
		if err := json.Unmarshal(cfg, &config); err != nil {
			return nil, fmt.Errorf("failed to parse config: %v", err)
		}
	}
	config.fillDefaultValues()

	logger.Info("NewPipelineTracer", "config", config)

	t := &PipelineTracer{
		config: config,
	}
	return t, nil
}

func (t *PipelineTracer) OnBlockchainInit(chainConfig *params.ChainConfig) {
	t.chainConfig = chainConfig
	logger.Info("Init pipeline with param", "chainConfig", chainConfig.ChainID.String(), "config", t.config)

	// set default election key
	if t.config.ElectionKey == "" {
		if t.config.Version == "" {
			t.config.ElectionKey = fmt.Sprintf("%s/writers/leader", chainConfig.ChainID.String())
		} else {
			t.config.ElectionKey = fmt.Sprintf("%s/%s/writers/leader", chainConfig.ChainID.String(), t.config.Version)
		}
	}

	if t.config.Topic == "" {
		if t.config.Version == "" {
			t.config.Topic = fmt.Sprintf("nodex_pipeline_%d", chainConfig.ChainID)
		} else {
			t.config.Topic = fmt.Sprintf("nodex_pipeline_%d_%s", chainConfig.ChainID, t.config.Version)
		}
	}

	// Initialize pipeline
	err := InitPipeline(t.config.Region, t.config.NodeXBucket, t.config.ChainTableBucket,
		t.config.Brokers, t.config.Topic, chainConfig.ChainID.String(), t.config.Version,
		t.config.S3TempDir)

	if err != nil {
		logger.Crit("Failed to init pipeline", "err", err)
	}

	// Prepare writer registry configuration
	var writerConfig *WriterRegistryConfig
	// Writer registry is always enabled when etcd endpoints are configured
	if len(t.config.EtcdEndpoints) > 0 {
		writerConfig = &WriterRegistryConfig{
			TTL:              t.config.WriterRegistryTTL,
			NodeXBucket:      t.config.NodeXBucket,
			ChainTableBucket: t.config.ChainTableBucket,
			Region:           t.config.Region,
			Brokers:          t.config.Brokers,
			Topic:            t.config.Topic,
		}
	}

	// Setup leader election based on configuration
	err = SetupLeaderElection(t.config.EtcdEndpoints, t.config.ElectionKey,
		t.config.NodeID, t.config.Version, t.config.IsBackup, t.config.GracePeriod, writerConfig)
	if err != nil {
		logger.Crit("Failed to setup leader election", "err", err)
		// Continue without election - will remain in backup mode
	}

	// start upload work should be after leader election
	err = NodeXPusher.StartUploadWork()
	if err != nil {
		logger.Crit("Failed to start NodeXPusher upload work", "err", err)
	}
	err = ChainTableBucketPusher.StartUploadWork()
	if err != nil {
		logger.Crit("Failed to start ChainTableBucketPusher upload work", "err", err)
	}
}

func (t *PipelineTracer) OnClose() {
	// Unregister writer node if registered
	if WriterRegistry != nil {
		if err := WriterRegistry.UnregisterNode(); err != nil {
			logger.Error("Failed to unregister writer node during shutdown", "err", err)
		} else {
			logger.Info("Writer node unregistered during shutdown")
		}
	}

	// Close leader manager if it exists
	if LeaderManager != nil {
		LeaderManager.Close()
		// Clear global reference
		leader.GlobalManager = nil
	}
	// Close processors
	if NodeXPusher != nil {
		NodeXPusher.Close()
	}
	if ChainTableBucketPusher != nil {
		ChainTableBucketPusher.Close()
	}
}

func (t *PipelineTracer) OnBlockStart(block *types.Block) {
	BlockCtx = &ExtraInfo{
		BlockNumber: block.NumberU64(),
		BlockHash:   block.Hash(),
	}
	BlockCtx.Header = block.Header()
	BlockCtx.BlockDiff = &ptypes.BlockStorageDiff{}
	BlockCtx.BlockHeader = util.BuildPilelineBlockHeader(block)
	BlockCtx.BlockFile = &ptypes.BlockFile{
		Block:            util.BuildPipelineBlock(block),
		Events:           make([]ptypes.Event, 0),
		Txs:              make([]ptypes.Transaction, 0),
		Traces:           make([]ptypes.Trace, 0),
		ErrorEvents:      make([]ptypes.Event, 0),
		ErrorTraces:      make([]ptypes.Trace, 0),
		StorageContracts: make([]string, 0),
	}
	BlockCtx.Tx = nil
	BlockCtx.From = common.Address{}
	BlockCtx.BlockStartTime = time.Now()
	BlockCtx.Committed = false
	BlockCtx.ChangeContracts = make(map[common.Address]struct{})
}

// OnBlockEnd finishes the block started by OnBlockStart. blockErr is the
// processing/validation/write error of the block, if any; a failed block is
// never uploaded.
func (t *PipelineTracer) OnBlockEnd(blockErr error) {
	if BlockCtx == nil {
		return
	}
	if blockErr != nil {
		logger.Warn("Skip uploading failed block", "number", BlockCtx.BlockNumber, "hash", BlockCtx.BlockHash, "err", blockErr)
		return
	}

	// empty block process
	if !BlockCtx.Committed {
		t.OnCommit(BlockCtx.BlockHeader.StateRoot, BlockCtx.BlockHeader.StateRoot, nil, nil, nil, nil, nil, nil)
	}

	// push block change notification
	if BlockCtx.BlockChange != nil {
		start := time.Now()
		err := NodeXPusher.PushBlockChangeNotification(BlockCtx.BlockChange, nil)
		if err == nil {
			logger.Info("Push kafka", "dropBlocks", BlockCtx.BlockChange.DropBlocks, "newBlocks", BlockCtx.BlockChange.NewBlocks, "kafka elapsed", common.PrettyDuration(time.Since(start)))
		} else {
			logger.Error("Failed to push kafka", "err", err, "dropBlocks", BlockCtx.BlockChange.DropBlocks, "newBlocks", BlockCtx.BlockChange.NewBlocks)
		}
	}
	metrics.BlockProcessTimer.UpdateSince(BlockCtx.BlockStartTime)
}

func (t *PipelineTracer) OnTxStart(tx *types.Transaction, from common.Address) {
	callTracer := newCallTracerRaw(BlockCtx.ChangeContracts, BlockCtx.BlockFile)
	t.callTracer = callTracer
	t.callTracer.OnTxStart(tx, from)

	BlockCtx.Tx = tx
	BlockCtx.From = from
	BlockCtx.TxStartTime = time.Now()
}

func (t *PipelineTracer) OnTxEnd(receipt *types.Receipt, err error) {
	defer func() {
		metrics.BlockTxExecutionTimer.UpdateSince(BlockCtx.TxStartTime)
	}()
	t.callTracer.OnTxEnd(receipt, err)
	t.callTracer = nil

	// The tx failed validation, so the whole block is invalid and won't be uploaded.
	if err != nil || receipt == nil {
		return
	}

	tx := util.BuildPipelineTransaction(BlockCtx.Tx, receipt, BlockCtx.From, BlockCtx.Header, t.chainConfig)
	BlockCtx.BlockFile.Txs = append(BlockCtx.BlockFile.Txs, tx)
}

func (t *PipelineTracer) CaptureTxStart(gasLimit uint64) {}

func (t *PipelineTracer) CaptureTxEnd(restGas uint64) {}

func (t *PipelineTracer) CaptureStart(env *vm.EVM, from common.Address, to common.Address, create bool, input []byte, gas uint64, value *big.Int) {
	if t.callTracer != nil {
		t.callTracer.CaptureStart(env, from, to, create, input, gas, value)
	}
}

func (t *PipelineTracer) CaptureEnd(output []byte, gasUsed uint64, err error) {
	if t.callTracer != nil {
		t.callTracer.CaptureEnd(output, gasUsed, err)
	}
}

func (t *PipelineTracer) CaptureEnter(typ vm.OpCode, from common.Address, to common.Address, input []byte, gas uint64, value *big.Int) {
	if t.callTracer != nil {
		t.callTracer.CaptureEnter(typ, from, to, input, gas, value)
	}
}

func (t *PipelineTracer) CaptureExit(output []byte, gasUsed uint64, err error) {
	if t.callTracer != nil {
		t.callTracer.CaptureExit(output, gasUsed, err)
	}
}

func (t *PipelineTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost, ccLeft, ccOpcode uint64, scope *vm.ScopeContext, depth int, err error) {
	if t.callTracer != nil {
		t.callTracer.CaptureState(env, pc, op, gas, cost, ccLeft, ccOpcode, scope, depth, err)
	}
}

func (t *PipelineTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost, ccLeft, ccOpcode uint64, scope *vm.ScopeContext, depth int, err error) {
	if t.callTracer != nil {
		t.callTracer.CaptureFault(env, pc, op, gas, cost, ccLeft, ccOpcode, scope, depth, err)
	}
}

func (t *PipelineTracer) OnLog(log *types.Log) {
	if t.callTracer != nil {
		t.callTracer.OnLog(log)
	}
}

// genesisTxID 构造 genesis 合成 tx 的 id: 0x + 2位类型码 + 22个0 + 小写地址(去0x, 40字符), 总长66字符,
// 与真实 tx hash 等长, 且为合法 hex, 可解析为 bytes32.
// kind: 1=alloc balance transfer, 2=alloc code create, 3=native token create
func genesisTxID(kind int, addrLower string) string {
	return fmt.Sprintf("0x%02d%022d%s", kind, 0, strings.TrimPrefix(addrLower, "0x"))
}

func (t *PipelineTracer) OnGenesisBlock(block *types.Block, alloc ptypes.GenesisAlloc) {
	if NodeXPusher.LastBlockNotice != nil {
		return
	}

	// 内部s3
	header := util.BuildPilelineBlockHeader(block)
	err := uploadBlockHeader(header)
	if err != nil {
		logger.Crit("Failed to upload block", "err", err)
	}
	logger.Info("[inner s3] 1.upload genesis block", "block hash", block.Hash().Hex(), "block number", block.Number().Uint64())

	blockDiff := GenesisAllocToStateDiff(alloc)
	blockDiff.Hash = block.Root()
	// genesis block has no parent
	blockDiff.ParentHash = types.EmptyRootHash
	err = uploadBlockDiff(blockDiff)
	if err != nil {
		logger.Crit("Failed to upload block diff files to s3", "err", err)
	}
	logger.Info("[inner s3] 2.upload genesis state diff", "block", block.Hash().Hex())

	// 业务s3
	blockFile := &ptypes.BlockFile{
		Block:            util.BuildPipelineBlock(block),
		Txs:              make([]ptypes.Transaction, 0),
		Events:           make([]ptypes.Event, 0),
		Traces:           make([]ptypes.Trace, 0),
		ErrorEvents:      make([]ptypes.Event, 0),
		ErrorTraces:      make([]ptypes.Trace, 0),
		StorageContracts: make([]string, 0),
	}

	// 构造 genesis tx 和 trace
	zeroAddr := "0x0000000000000000000000000000000000000000"
	txIdx := int64(0)

	// 对地址排序，确保遍历顺序确定性
	sortedAddrs := make([]common.Address, 0, len(alloc))
	for addr := range alloc {
		sortedAddrs = append(sortedAddrs, addr)
	}
	sort.Slice(sortedAddrs, func(i, j int) bool {
		return sortedAddrs[i].Hex() < sortedAddrs[j].Hex()
	})

	for _, addr := range sortedAddrs {
		account := alloc[addr]
		addrLower := strings.ToLower(addr.Hex())

		// 处理有 Storage 的账户
		if len(account.Storage) > 0 {
			blockFile.StorageContracts = append(blockFile.StorageContracts, addrLower)
		}

		// 处理有 balance 的账户 - 构造转账 tx 和 call trace
		if account.Balance != nil && account.Balance.Sign() > 0 {
			// tx id: 0x + 01 + 22个0 + 地址(去0x, 40字符) = 66字符, 可解析为 bytes32
			txID := genesisTxID(1, addrLower)

			tx := ptypes.Transaction{
				ID:               txID,
				From:             zeroAddr,
				To:               addrLower,
				Gas:              big.NewInt(0),
				GasPrice:         big.NewInt(0),
				GasUsed:          big.NewInt(0),
				Status:           true,
				GasFeeCap:        big.NewInt(0),
				GasTipCap:        big.NewInt(0),
				Input:            []byte{},
				Nonce:            big.NewInt(0),
				TransactionIndex: txIdx,
				Value:            (*hexutil.Big)(account.Balance),
			}
			blockFile.Txs = append(blockFile.Txs, tx)

			// trace id = hash(tx_id, parent_trace_id, pos_in_parent_trace)
			traceID := util.ToHash([]string{txID, "", "0"})
			trace := ptypes.Trace{
				ID:                traceID,
				From:              zeroAddr,
				Gas:               big.NewInt(0),
				Input:             []byte{},
				To:                addrLower,
				Value:             (*hexutil.Big)(account.Balance),
				GasUsed:           big.NewInt(0),
				Output:            []byte{},
				CallCreateType:    "call",
				CallType:          "call",
				TxID:              txID,
				ParentTraceID:     "",
				PosInParentTrace:  0,
				SelfStorageChange: false,
				StorageChange:     false,
				Subtraces:         0,
				TraceAddress:      []int64{},
			}
			blockFile.Traces = append(blockFile.Traces, trace)
			txIdx++
		}

		// 处理有 code 的账户 - 构造 create tx 和 create trace
		if len(account.Code) > 0 {
			// tx id: 0x + 02 + 22个0 + 地址(去0x, 40字符) = 66字符, 可解析为 bytes32
			txID := genesisTxID(2, addrLower)

			tx := ptypes.Transaction{
				ID:               txID,
				From:             zeroAddr,
				To:               addrLower,
				Gas:              big.NewInt(0),
				GasPrice:         big.NewInt(0),
				GasUsed:          big.NewInt(0),
				Status:           true,
				GasFeeCap:        big.NewInt(0),
				GasTipCap:        big.NewInt(0),
				Input:            account.Code,
				Nonce:            big.NewInt(0),
				TransactionIndex: txIdx,
				Value:            (*hexutil.Big)(big.NewInt(0)),
			}
			blockFile.Txs = append(blockFile.Txs, tx)

			// trace id = hash(tx_id, parent_trace_id, pos_in_parent_trace)
			traceID := util.ToHash([]string{txID, "", "0"})
			trace := ptypes.Trace{
				ID:                traceID,
				From:              zeroAddr,
				Gas:               big.NewInt(0),
				Input:             account.Code,
				To:                addrLower,
				Value:             (*hexutil.Big)(big.NewInt(0)),
				GasUsed:           big.NewInt(0),
				Output:            account.Code, // output 直接使用 input (code)
				CallCreateType:    "create",
				CallType:          "",
				TxID:              txID,
				ParentTraceID:     "",
				PosInParentTrace:  0,
				SelfStorageChange: false,
				StorageChange:     false,
				Subtraces:         0,
				TraceAddress:      []int64{},
			}
			blockFile.Traces = append(blockFile.Traces, trace)
			txIdx++
		}
	}

	// 添加原生代币合约创建 tx 和 trace (E地址)
	nativeTokenAddr := "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	// tx id: 0x + 03 + 22个0 + 地址(去0x, 40字符) = 66字符, 可解析为 bytes32
	nativeTokenTxID := genesisTxID(3, nativeTokenAddr)

	nativeTokenTx := ptypes.Transaction{
		ID:               nativeTokenTxID,
		From:             zeroAddr,
		To:               nativeTokenAddr,
		Gas:              big.NewInt(0),
		GasPrice:         big.NewInt(0),
		GasUsed:          big.NewInt(0),
		Status:           true,
		GasFeeCap:        big.NewInt(0),
		GasTipCap:        big.NewInt(0),
		Input:            []byte{},
		Nonce:            big.NewInt(0),
		TransactionIndex: txIdx,
		Value:            (*hexutil.Big)(big.NewInt(0)),
	}
	blockFile.Txs = append(blockFile.Txs, nativeTokenTx)

	nativeTokenTraceID := util.ToHash([]string{nativeTokenTxID, "", "0"})
	nativeTokenTrace := ptypes.Trace{
		ID:                nativeTokenTraceID,
		From:              zeroAddr,
		Gas:               big.NewInt(0),
		Input:             []byte{},
		To:                nativeTokenAddr,
		Value:             (*hexutil.Big)(big.NewInt(0)),
		GasUsed:           big.NewInt(0),
		Output:            []byte{},
		CallCreateType:    "create",
		CallType:          "",
		TxID:              nativeTokenTxID,
		ParentTraceID:     "",
		PosInParentTrace:  0,
		SelfStorageChange: false,
		StorageChange:     false,
		Subtraces:         0,
		TraceAddress:      []int64{},
	}
	blockFile.Traces = append(blockFile.Traces, nativeTokenTrace)

	// upload block file and meta data
	err = uploadBlockFile(blockFile)
	if err != nil {
		logger.Crit("Failed to upload block files to s3", "err", err)
	}
	logger.Info("3.upload block file", "block hash", header.Hash.Hex(), "block number", header.Number.ToInt().Uint64(),
		"txs", len(blockFile.Txs), "traces", len(blockFile.Traces))

	// upload block file validation
	err = uploadblockFileValidation(blockFile)
	if err != nil {
		logger.Crit("Failed to upload file validation to s3", "err", err)
	}
	logger.Info("4.upload block file validation", "block hash", header.Hash.Hex(), "block number", header.Number.ToInt().Uint64())

	// push block change notification
	blockChanges := &ptypes.BlockChangeNotification{
		ChangeType: 1,
		NewBlocks: []ptypes.BlockContext{
			{
				Hash:        block.Hash(),
				ParentHash:  block.ParentHash(),
				BlockNumber: block.NumberU64(),
				Timestamp:   block.Time().Uint64(),
			},
		},
	}

	err = NodeXPusher.PushBlockChangeNotification(blockChanges, nil)
	if err != nil {
		logger.Crit("Failed to push block change notification", "err", err)
	}

	logger.Info("push genesis block change notification", "block hash", block.Hash().Hex(), "block number", block.Number().Uint64())
}

func (t *PipelineTracer) OnCommit(originRoot common.Hash, root common.Hash, destructs map[common.Hash]struct{}, accounts map[common.Hash][]byte, accountsOrigin map[common.Address][]byte, storages map[common.Hash]map[common.Hash][]byte, storagesOrigin map[common.Address]map[common.Hash][]byte, codes map[common.Hash][]byte) {
	if originRoot != root {
		BlockCtx.BlockDiff = stateUpdateToStateDiff(originRoot, root, destructs, accounts, accountsOrigin, storages, storagesOrigin, codes)
	} else {
		BlockCtx.BlockDiff = nil
	}

	for addr := range BlockCtx.ChangeContracts {
		BlockCtx.BlockFile.StorageContracts = append(BlockCtx.BlockFile.StorageContracts, strings.ToLower(addr.Hex()))
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var uploadErrs []error

	// Helper function to handle errors safely
	handleError := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			uploadErrs = append(uploadErrs, err)
		}
	}

	// 上传 block head
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := uploadBlockHeader(BlockCtx.BlockHeader)
		if err != nil {
			handleError(err)
			return
		}
	}()

	// 上传 state diff
	wg.Add(1)
	go func() {
		defer wg.Done()
		if BlockCtx.BlockDiff == nil {
			return
		}
		err := uploadBlockDiff(BlockCtx.BlockDiff)
		if err != nil {
			handleError(err)
			return
		}
	}()

	// 上传 block file
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := uploadBlockFile(BlockCtx.BlockFile)
		if err != nil {
			handleError(err)
			return
		}
	}()

	// 上传 block file validation
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := uploadblockFileValidation(BlockCtx.BlockFile)
		if err != nil {
			handleError(err)
			return
		}
	}()

	// 等待所有上传完成
	wg.Wait()

	logger.Info("Upload block", "block number", BlockCtx.BlockNumber, "block hash", BlockCtx.BlockHash.Hex())

	// 检查是否有错误
	if len(uploadErrs) > 0 {
		for _, err := range uploadErrs {
			logger.Error("Upload error", "err", err)
		}
		logger.Crit("One or more uploads failed")
	}

	BlockCtx.Committed = true

	metrics.LatestUploadedBlockNumber.Update(int64(BlockCtx.BlockNumber))
}
