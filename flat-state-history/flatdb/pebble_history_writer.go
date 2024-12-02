package flatdb

import (
	"errors"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/flat-state-history/traits"
	"github.com/kaiachain/kaia/log"
	"github.com/kaiachain/kaia/rlp"
	"github.com/kaiachain/kaia/storage/database"
)

var logger = log.NewModuleLogger(log.Flatdb)

var _ traits.HistoryWriter = (*PebbleHistoryWriter)(nil)

var writer *PebbleHistoryWriter

type PebbleHistoryWriter struct {
	db                    *pebble.DB
	mu                    sync.Mutex
	mapDiff               map[uint64][]*traits.BlockStorageDiff
	lastCommitBlockNumber uint64
	lastCommitRootHash    common.Hash
	shouldAppendDiff      bool
	appendDiffOnly        bool
	maxForkDepth          uint64
}

func GetWriter() *PebbleHistoryWriter {
	if store == nil {
		return nil
	}
	return writer
}

func NewPebbleStateWriter(db *pebble.DB, maxForkDepth uint64, appendOnly bool) *PebbleHistoryWriter {
	reader := NewPebbleHistoryReader(db)
	blockNumber, root, err := reader.GetLatestStateNumber()
	if err != nil {
		if errors.Is(err, traits.ErrBlockNotFound) {
			blockNumber = 0
			root = traits.EmptyRootHash
		} else {
			panic(err)
		}
	}
	if root == (common.Hash{}) {
		root = traits.EmptyRootHash
	}
	mapDiff := make(map[uint64][]*traits.BlockStorageDiff)
	if !appendOnly {
		mapDiff, err = reader.GetBlockDiffAfter(blockNumber)
		if err != nil {
			panic(err)
		}
	}
	logger.Info("NewPebbleStateWriter", "blockNumber", blockNumber, "mapDiff", len(mapDiff), "root", root.Hex())
	return &PebbleHistoryWriter{db: db, mapDiff: mapDiff, lastCommitBlockNumber: blockNumber, lastCommitRootHash: root, maxForkDepth: maxForkDepth, appendDiffOnly: appendOnly}
}

func (w *PebbleHistoryWriter) writeBlockRoot(batch *pebble.Batch, blockNumber uint64, root common.Hash) error {
	key := append(BlockRootPrefix, EncodeBlockNumber(blockNumber)...)
	batch.Set(key, root.Bytes(), nil)
	return nil
}

func (w *PebbleHistoryWriter) writeNewAccount(batch *pebble.Batch, blockNumber uint64, account *traits.NewAccount) error {
	acckey := append(AccountPrefix, account.Address.Bytes()...)
	accNumkey := append(acckey, EncodeBlockNumber(blockNumber)...)
	slimAccount := &SlimAccount{
		Balance:  account.Balance,
		Nonce:    account.Nonce,
		CodeHash: account.CodeHash,
		Root:     account.Root,
	}
	bytes, _ := rlp.EncodeToBytes(slimAccount)
	batch.Set(accNumkey, bytes, nil)
	return nil
}

func (w *PebbleHistoryWriter) writeDeletedAccount(batch *pebble.Batch, blockNumber uint64, address common.Hash) error {
	acckey := append(AccountPrefix, address.Bytes()...)
	accNumkey := append(acckey, EncodeBlockNumber(blockNumber)...)
	batch.Set(accNumkey, nil, nil)
	return nil
}

func (w *PebbleHistoryWriter) writeStorage(batch *pebble.Batch, blockNumber uint64, address common.Hash, storage traits.IndexValuePair) error {
	storageKey := append(StoragePrefix, address.Bytes()...)
	storageKey = append(storageKey, storage.Index.Bytes()...)
	storageNumKey := append(storageKey, EncodeBlockNumber(blockNumber)...)
	batch.Set(storageNumKey, storage.Value.Bytes(), nil)
	return nil
}

func (w *PebbleHistoryWriter) writeUnCommitedDiffs() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.appendDiffOnly {
		return nil
	}
	if w.shouldAppendDiff {
		return nil
	}
	w.shouldAppendDiff = true

	batch := w.db.NewBatch()
	defer batch.Close()
	for blockNumber := range w.mapDiff {
		if blockNumber <= w.lastCommitBlockNumber {
			delete(w.mapDiff, blockNumber)
		}
	}
	startKey := append(DiffPrefix, EncodeBlockNumber(1)...)
	endKey := append(DiffPrefix, EncodeBlockNumber(w.lastCommitBlockNumber)...)
	err := batch.DeleteRange(startKey, endKey, nil)
	if err != nil {
		return err
	}
	for blockNumber, diffs := range w.mapDiff {
		for _, diff := range diffs {
			buf, err := rlp.EncodeToBytes(diff)
			if err != nil {
				return err
			}
			diffKey := append(DiffPrefix, EncodeBlockNumber(blockNumber)...)
			diffKey = append(diffKey, diff.Hash.Bytes()...)
			batch.Set(diffKey, buf, nil)
			if batch.Len() > database.IdealBatchSize {
				if err := batch.Commit(nil); err != nil {
					return err
				}
				batch.Reset()
			}
		}
	}
	if err := batch.Commit(nil); err != nil {
		return err
	}
	return nil
}

func (w *PebbleHistoryWriter) appendDiff(batch *pebble.Batch, blockNumber uint64, diff *traits.BlockStorageDiff) error {
	buf, err := rlp.EncodeToBytes(diff)
	if err != nil {
		return err
	}

	diffKey := append(DiffPrefix, EncodeBlockNumber(blockNumber)...)
	diffKey = append(diffKey, diff.Hash.Bytes()...)
	batch.Set(diffKey, buf, nil)
	return nil
}

func (w *PebbleHistoryWriter) applyDiff(batch *pebble.Batch, blockNumber uint64, diff *traits.BlockStorageDiff) error {
	if w.lastCommitRootHash != diff.ParentHash {
		logger.Error("WriteStateDiff", "lastCommitRootHash", w.lastCommitRootHash, "diff.ParentHash", diff.ParentHash)
		return fmt.Errorf("lastCommitRootHash %s != diff.ParentHash %s", w.lastCommitRootHash, diff.ParentHash)
	}
	root := diff.Hash
	if err := w.writeBlockRoot(batch, blockNumber, root); err != nil {
		return err
	}
	// must write delete first
	deletedCount := 0
	for _, address := range diff.DeletedAccounts {
		deletedCount++
		if err := w.writeDeletedAccount(batch, blockNumber, address); err != nil {
			return err
		}
	}
	accountCount := 0
	for _, account := range diff.NewAccounts {
		accountCount++
		if err := w.writeNewAccount(batch, blockNumber, &account); err != nil {
			return err
		}
	}
	storageCount := 0
	for _, storage := range diff.StorageDiff {
		for _, indexValuePair := range storage.Values {
			storageCount++
			if err := w.writeStorage(batch, blockNumber, storage.Address, indexValuePair); err != nil {
				return err
			}
		}
	}
	w.lastCommitBlockNumber = blockNumber
	w.lastCommitRootHash = root
	logger.Info("WriteStateDiff", "blockNumber", blockNumber, "root", root, "account", accountCount, "deleted", deletedCount, "storage", storageCount)
	if blockNumber%1000 == 0 {
		m := w.db.Metrics()
		logger.Info("memtbl stats", "memtbl count", m.MemTable.Count, "memtbl size", m.MemTable.Size/(1024*1024))
		logger.Info("zmemtbl stats", "zmemrbl count", m.MemTable.ZombieCount, "zmemtbl size", m.MemTable.ZombieSize/(1024*1024))
		logger.Info("ztbl stat", "zcount", m.Table.ZombieCount, "zsize", m.Table.ZombieSize/(1024*1024))
		logger.Info("BlockCache", "bcount", m.BlockCache.Count, "bsize", m.BlockCache.Size/(1024*1024))
		logger.Info("TableCache", "tcount", m.TableCache.Count, "tsize", m.TableCache.Size/(1024*1024))
		fmt.Println(m.String())
	}
	return nil
}

type BlockStorageDiffWithNumber struct {
	BlockNumber uint64
	Diff        *traits.BlockStorageDiff
}

func (w *PebbleHistoryWriter) ApplyDiffs(batch *pebble.Batch, blockNumber uint64, diff *traits.BlockStorageDiff) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.shouldAppendDiff || w.appendDiffOnly {
		if err := w.appendDiff(batch, blockNumber, diff); err != nil {
			return err
		}
	}
	if w.appendDiffOnly {
		return nil
	}
	w.mapDiff[blockNumber] = append(w.mapDiff[blockNumber], diff)
	if w.lastCommitRootHash == traits.EmptyRootHash && blockNumber == 0 {
		return w.applyDiff(batch, blockNumber, diff)
	}
	if blockNumber <= w.lastCommitBlockNumber+w.maxForkDepth {
		// block reorg
		logger.Debug("block reorg", "blockNumber", blockNumber, "lastCommitBlockNumber", w.lastCommitBlockNumber, "maxCanCommit", blockNumber-w.maxForkDepth)
		return nil
	}
	// shortcut
	if blockNumber == w.lastCommitBlockNumber+w.maxForkDepth+1 && len(w.mapDiff[w.lastCommitBlockNumber+1]) == 1 {
		return w.applyDiff(batch, w.lastCommitBlockNumber+1, w.mapDiff[w.lastCommitBlockNumber+1][0])
	}
	child := diff
	path := make([]*BlockStorageDiffWithNumber, 0)
	for start := blockNumber - 1; start > w.lastCommitBlockNumber; start-- {
		parents := w.mapDiff[start]
		if len(parents) == 0 {
			logger.Info("flatdb skip empty block", "blockNumber", start)
			if start == w.lastCommitBlockNumber+1 {
				if child.ParentHash == w.lastCommitRootHash {
					path = append(path, &BlockStorageDiffWithNumber{BlockNumber: start, Diff: &traits.BlockStorageDiff{
						Hash:       w.lastCommitRootHash,
						ParentHash: w.lastCommitRootHash,
					}})
					break
				}
			}
			continue
		}
		foundParent := false
		for _, parent := range parents {
			if child.ParentHash == parent.Hash {
				if start <= blockNumber-w.maxForkDepth {
					path = append(path, &BlockStorageDiffWithNumber{BlockNumber: start, Diff: parent})
				}
				child = parent
				foundParent = true
				break
			}
		}
		if !foundParent {
			logger.Error("flat parent not found", "blockNumber", start, "child.Hash", child.Hash.String(), "child.ParrentHash", child.ParentHash.String())
			return fmt.Errorf("parent not found")
		}
	}
	if len(path) == 0 {
		logger.Error("apply path is zero", "blockNumber", blockNumber, "lastCommitBlockNumber", w.lastCommitBlockNumber)
		return fmt.Errorf("path not found")
	}
	for i := len(path) - 1; i >= 0; i-- {
		if err := w.applyDiff(batch, path[i].BlockNumber, path[i].Diff); err != nil {
			return err
		}
	}
	return nil
}

func (w *PebbleHistoryWriter) Cap() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for blockNumber := range w.mapDiff {
		if blockNumber <= w.lastCommitBlockNumber {
			delete(w.mapDiff, blockNumber)
		}
	}
	return nil
}

func (w *PebbleHistoryWriter) WriteStateDiff(blockNumber uint64, diff *traits.BlockStorageDiff) error {
	batch := w.db.NewBatch()
	defer batch.Close()
	err := w.ApplyDiffs(batch, blockNumber, diff)
	if err != nil {
		return err
	}
	if err := batch.Commit(nil); err != nil {
		return err
	}
	if err := w.Cap(); err != nil {
		return err
	}
	return nil
}
