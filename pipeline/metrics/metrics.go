package metrics

import (
	"fmt"

	"github.com/rcrowley/go-metrics"
)

var (
	LatestBlockNumber = metrics.NewRegisteredGauge("pipeline/block_num", nil)

	LatestBlockTime = metrics.NewRegisteredGauge("pipeline/block_time", nil)

	LatestUploadedBlockNumber = metrics.NewRegisteredGauge("pipeline/latest_uploaded_block_number", nil)

	BlockProcessTimer = metrics.NewRegisteredTimer("pipeline/block_process", nil)

	BlockTxExecutionTimer = metrics.NewRegisteredTimer("pipeline/tx_execution", nil)

	BlockHeaderUploadTimer = metrics.NewRegisteredTimer("pipeline/block_header_upload", nil)

	StateDiffUploadTimer = metrics.NewRegisteredTimer("pipeline/state_diff_upload", nil)

	BlockFileUploadTimer = metrics.NewRegisteredTimer("pipeline/block_file_upload", nil)

	BlockFileValidationTimer = metrics.NewRegisteredTimer("pipeline/block_file_validation", nil)

	BlockPushTimer = metrics.NewRegisteredTimer("pipeline/block_push", nil)

	// S3UploadRetryCounter 累计 S3 上传重试次数，限流抬头即可告警。
	S3UploadRetryCounter = metrics.NewRegisteredCounter("pipeline/s3_upload_retry", nil)
)

// KafkaWriteTimer returns the write timer for a Kafka topic. The underlying
// metrics registry does not support labels, so the topic is part of the metric
// name (for example, pipeline/kafka_write/orders).
func KafkaWriteTimer(topic string) metrics.Timer {
	return metrics.GetOrRegisterTimer(fmt.Sprintf("pipeline/kafka_write/%s", topic), nil)
}
