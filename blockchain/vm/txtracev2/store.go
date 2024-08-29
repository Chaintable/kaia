package txtracev2

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/rlp"
	"github.com/kaiachain/kaia/storage/database"
	"github.com/rcrowley/go-metrics"
)

// Store contains all the methods for tx-trace to interact with the underlying database.
type Store interface {
	// ReadTxTrace retrieve tracing result from underlying database.
	ReadTxTrace(ctx context.Context, txHash common.Hash) ([]byte, error)
	// WriteTxTrace write tracing result to underlying database.
	WriteTxTrace(ctx context.Context, txHash common.Hash, trace []byte) error
}

var (
	txTraceWriteSuccessCounter = metrics.NewRegisteredCounter("chain/txtraces/write/success", nil)
	txTraceWriteFailCounter    = metrics.NewRegisteredCounter("chain/txtraces/write/fail", nil)
)

var (
	once              sync.Once
	defaultTraceStore *traceStore
)

// ReadRpcTxTrace reads internal tx-trace from underlying database and decodes it to rpc-tx-trace.
func ReadRpcTxTrace(ctx context.Context, store Store, txHash common.Hash) (ActionTraceList, error) {
	raw, err := store.ReadTxTrace(ctx, txHash)
	if err != nil {
		return nil, err
	}
	if bytes.Equal(raw, []byte{}) { // empty response
		return nil, fmt.Errorf("trace result of tx {%#v} not found in tracedb", txHash)
	}
	txs := ActionTraceList{}
	err = rlp.DecodeBytes(raw, &txs)
	if err != nil {
		return nil, fmt.Errorf("failed to decode rlp traces: %v", err)
	}
	return txs, nil
}

type traceStore struct {
	dbm database.DBManager
}

// NewTraceStore creates a new trace store.
func NewTraceStore(dbm database.DBManager) *traceStore {
	if defaultTraceStore != nil {
		return defaultTraceStore
	}
	once.Do(func() {
		defaultTraceStore = &traceStore{dbm: dbm}
	})
	return defaultTraceStore
}

func (t *traceStore) guard() error {
	if t.dbm == nil {
		return fmt.Errorf("txtrace mode not enabled")
	}
	return nil
}

// ReadTxTrace retrieves the result of tx by evm-tracing which stores in db.
func (t *traceStore) ReadTxTrace(ctx context.Context, txHash common.Hash) ([]byte, error) {
	if err := t.guard(); err != nil {
		return []byte{}, err
	}
	data := t.dbm.ReadTxTrace(txHash)
	return data, nil
}

// WriteTxTrace write the result of tx tracing by evm-tracing to db.
func (t *traceStore) WriteTxTrace(ctx context.Context, txHash common.Hash, trace []byte) error {
	if err := t.guard(); err != nil {
		return err
	}
	err := t.dbm.WriteTxTrace(txHash, trace)
	if err == nil {
		txTraceWriteSuccessCounter.Inc(1)
		return nil
	}
	txTraceWriteFailCounter.Inc(1)
	return err
}
