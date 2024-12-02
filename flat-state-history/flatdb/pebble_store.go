package flatdb

import (
	"runtime"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

var store *pebble.DB

func Init(path string, dbCacheSize int64, maxForkDepth uint64, appendOnly bool) error {
	if dbCacheSize == 0 {
		dbCacheSize = 256
	}
	memTableLimit := 2
	memTableSize := int(dbCacheSize) * 1024 * 1024 / 2 / memTableLimit
	opt := &pebble.Options{
		// Pebble has a single combined cache area and the write
		// buffers are taken from this too. Assign all available
		// memory allowance for cache.
		Cache: pebble.NewCache(int64(dbCacheSize * 1024 * 1024)),

		// The size of memory table(as well as the write buffer).
		// Note, there may have more than two memory tables in the system.
		MemTableSize: uint64(memTableSize),

		MemTableStopWritesThreshold: memTableLimit,

		// The default compaction concurrency(1 thread),
		// Here use all available CPUs for faster compaction.
		MaxConcurrentCompactions: func() int { return runtime.NumCPU() },
		Levels: []pebble.LevelOptions{
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
		},
	}
	opt.Experimental.ReadSamplingMultiplier = -1
	db, err := pebble.Open(path, opt)
	if err != nil {
		return err
	}
	store = db
	writer = NewPebbleStateWriter(db, maxForkDepth, appendOnly)
	return nil
}

func BeforeClose() error {
	if store != nil {
		err := GetWriter().writeUnCommitedDiffs()
		if err != nil {
			logger.Error("flatdb writeUnCommitedDiffs", "err", err)
			return err
		}
		logger.Info("flatdb writeUnCommitedDiffs done")
	}
	return nil
}

func Close() error {
	if store != nil {
		if err := BeforeClose(); err != nil {
			return err
		}
		if err := store.Flush(); err != nil {
			logger.Error("store.Flush", "err", err)
			return err
		}
		logger.Info("flatdb close done")
		return store.Close()
	}
	return nil
}

func GetRawStore() *pebble.DB {
	return store
}
