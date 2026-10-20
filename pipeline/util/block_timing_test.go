package util

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/pipeline/types"
	"github.com/segmentio/kafka-go"
)

func TestBlockFirstSeenHeadersRoundTrip(t *testing.T) {
	firstHash := common.HexToHash("0x01")
	secondHash := common.HexToHash("0x02")
	firstSeen := time.Date(2026, time.August, 10, 12, 0, 0, 123*int(time.Millisecond), time.UTC)
	secondSeen := firstSeen.Add(2 * time.Second)

	headers, err := EncodeBlockFirstSeenHeaders(map[common.Hash]int64{
		firstHash:                firstSeen.UnixMilli(),
		secondHash:               secondSeen.UnixMilli(),
		common.HexToHash("0x03"): 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(headers) != 1 {
		t.Fatalf("EncodeBlockFirstSeenHeaders returned %d headers, want 1", len(headers))
	}
	if headers[0].Key != BlockP2PHeaderRecvKey {
		t.Fatalf("header key = %q, want %q", headers[0].Key, BlockP2PHeaderRecvKey)
	}
	var encoded map[common.Hash]int64
	if err := json.Unmarshal(headers[0].Value, &encoded); err != nil {
		t.Fatal(err)
	}
	if len(encoded) != 2 || encoded[firstHash] != firstSeen.UnixMilli() || encoded[secondHash] != secondSeen.UnixMilli() {
		t.Fatalf("encoded timings = %v", encoded)
	}

	timings := DecodeBlockFirstSeenHeaders(headers)
	if got := timings[firstHash]; !got.Equal(firstSeen) {
		t.Fatalf("first timing = %v, want %v", got, firstSeen)
	}
	if got := timings[secondHash]; !got.Equal(secondSeen) {
		t.Fatalf("second timing = %v, want %v", got, secondSeen)
	}
}

func TestDecodeBlockFirstSeenHeadersUsesEarliestValidValue(t *testing.T) {
	hash := common.HexToHash("0x01")
	early := time.UnixMilli(1_700_000_000_000)
	late := early.Add(time.Second)
	lateHeaders, err := EncodeBlockFirstSeenHeaders(map[common.Hash]int64{hash: late.UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	earlyHeaders, err := EncodeBlockFirstSeenHeaders(map[common.Hash]int64{hash: early.UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	headers := append(lateHeaders, earlyHeaders...)
	headers = append(headers,
		kafka.Header{Key: BlockP2PHeaderRecvKey, Value: []byte("malformed")},
		kafka.Header{Key: "unknown", Value: []byte(`{"ignored":true}`)},
	)

	if got := DecodeBlockFirstSeenHeaders(headers)[hash]; !got.Equal(early) {
		t.Fatalf("decoded timing = %v, want %v", got, early)
	}
}

func TestBlockContextPayloadHasNoFirstSeenTiming(t *testing.T) {
	block := types.BlockContext{
		Hash:        common.HexToHash("0x01"),
		ParentHash:  common.HexToHash("0x02"),
		BlockNumber: 12,
		Timestamp:   34,
	}
	data, err := json.Marshal(block)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded["firstSeenAtUnixMilli"]; ok {
		t.Fatal("first-seen timing leaked into the Kafka payload")
	}
	if len(decoded) != 4 {
		t.Fatalf("BlockContext payload has %d fields, want 4: %v", len(decoded), decoded)
	}
}
