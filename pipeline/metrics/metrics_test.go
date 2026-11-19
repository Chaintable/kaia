package metrics

import (
	"testing"

	gometrics "github.com/rcrowley/go-metrics"
)

func TestKafkaWriteTimerUsesTopicInMetricName(t *testing.T) {
	const firstTopic = "pipeline-metrics-test-one"
	const secondTopic = "pipeline-metrics-test-two"

	firstTimer := KafkaWriteTimer(firstTopic)
	if firstTimer != KafkaWriteTimer(firstTopic) {
		t.Fatal("expected the timer for a topic to be reused")
	}
	if firstTimer == KafkaWriteTimer(secondTopic) {
		t.Fatal("expected different topics to use different timers")
	}
	if gometrics.DefaultRegistry.Get("pipeline/kafka_write/"+firstTopic) != firstTimer {
		t.Fatal("topic timer was not registered with the topic in its metric name")
	}
}
