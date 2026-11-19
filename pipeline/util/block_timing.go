package util

import (
	"encoding/json"
	"time"

	"github.com/kaiachain/kaia/common"
	"github.com/segmentio/kafka-go"
)

const BlockP2PHeaderRecvKey = "block-p2p-header-recv"

// EncodeBlockFirstSeenHeaders creates one header containing a block hash to
// first-seen Unix millisecond map. The payload remains unchanged.
func EncodeBlockFirstSeenHeaders(firstSeenAt map[common.Hash]int64) ([]kafka.Header, error) {
	timings := make(map[common.Hash]int64)
	for hash, millis := range firstSeenAt {
		if millis <= 0 {
			continue
		}
		timings[hash] = millis
	}
	if len(timings) == 0 {
		return nil, nil
	}
	value, err := json.Marshal(timings)
	if err != nil {
		return nil, err
	}
	return []kafka.Header{{Key: BlockP2PHeaderRecvKey, Value: value}}, nil
}

// DecodeBlockFirstSeenHeaders returns the earliest valid timestamp for each
// block hash. Unknown and malformed headers are ignored for compatibility.
func DecodeBlockFirstSeenHeaders(headers []kafka.Header) map[common.Hash]time.Time {
	timings := make(map[common.Hash]time.Time)
	for _, header := range headers {
		if header.Key != BlockP2PHeaderRecvKey {
			continue
		}
		var encoded map[common.Hash]int64
		if err := json.Unmarshal(header.Value, &encoded); err != nil {
			continue
		}
		for hash, millis := range encoded {
			if millis <= 0 {
				continue
			}
			seenAt := time.UnixMilli(millis)
			if current, ok := timings[hash]; !ok || seenAt.Before(current) {
				timings[hash] = seenAt
			}
		}
	}
	return timings
}
