//go:build integration

package util

import (
	"context"
	"testing"

	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/pipeline/types"
)

func TestKafkaReader(t *testing.T) {
	reader := NewKafkaReader([]string{"b-1.chaintablenodexpi.pz2h9z.c4.kafka.ap-northeast-1.amazonaws.com:9092"}, "nodex_pipeline_test", "")
	reader.SetOffset(0)
	lag, a := reader.ReadLag(context.Background())
	if a != nil {
		t.Error(a)
	}
	t.Log(lag)

	msg, err := GetLastBlockNotice(reader)
	if err != nil {
		t.Error(err)
	}
	t.Log(msg)
}

func TestKafkaWriter(t *testing.T) {
	writer := NewKafkaWriter([]string{"localhost:9092"}, "nodex_pipeline_1_tmp")
	if writer == nil {
		t.Error("Failed to create Kafka writer")
	}
	err := WriteBlockNotice(writer, &types.BlockChangeNotification{
		ChangeType: 1,
		NewBlocks: []types.BlockContext{
			{
				Hash:        common.HexToHash("0x6976c80455f9f81526a7507114e1efe8e49558887e0aa7318b795853e5fe8ff6"),
				ParentHash:  common.HexToHash("0x64786bd9fefb480d4f3ed0b3f62b870e69c11bc6dc96c02c2ccc6f308bcb90c2"),
				BlockNumber: 5711598,
				Timestamp:   1527817842,
			},
		},
		DropBlocks: nil,
	}, nil)
	if err != nil {
		t.Error(err)
	}
	t.Log("write block notice success")
}

// 查看kafka的offset
func TestKafkaReader1(t *testing.T) {
	t.Log("1132")
	reader := NewKafkaReader([]string{"localhost:9092"}, "nodex_pipeline_1_tmp", "")
	reader.SetOffset(0)
	lag, a := reader.ReadLag(context.Background())
	if a != nil {
		t.Error(a)
	}
	t.Logf("lag: %d", lag)

	// 输出最后一条消息
	msg, err := GetLastBlockNotice(reader)
	if err != nil {
		t.Error(err)
	}
	t.Logf("last message: %+v", msg)
}
