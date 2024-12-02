package flatdb

import "github.com/cockroachdb/pebble"

func DeleteKeysWithBlockNumberGreaterThanFrom(db *pebble.DB, dir string, from uint64) (int, error) {
	iter, _ := db.NewIter(nil)
	defer iter.Close()

	batch := db.NewBatch()
	defer batch.Close()

	count := 0

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if key[0] != AccountPrefix[0] && key[0] != StoragePrefix[0] && key[0] != BlockRootPrefix[0] {
			continue
		}
		blockNumberBytes := key[len(key)-8:]
		blockNumber := DecodeBlockNumber(blockNumberBytes)
		if blockNumber >= from {
			count++
			err := batch.Delete(key, nil)
			if err != nil {
				return count, err
			}
		}
		if batch.Len() > 1024*64 {
			if err := batch.Commit(nil); err != nil {
				return count, err
			}
			batch.Reset()
		}
	}

	if err := batch.Commit(nil); err != nil {
		return count, err
	}

	return count, nil
}
