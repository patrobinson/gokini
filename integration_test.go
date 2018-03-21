//+build integration

package gokini

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	log "github.com/sirupsen/logrus"
)

type IntegrationRecordConsumer struct {
	shardID          string
	processedRecords map[string]int
}

func (p *IntegrationRecordConsumer) Init(shardID string) error {
	p.shardID = shardID
	return nil
}

func (p *IntegrationRecordConsumer) ProcessRecords(records []*Records, consumer *KinesisConsumer) {
	if len(records) > 0 {
		for _, record := range records {
			p.processedRecords[record.SequenceNumber] += 1
		}
	}
}

func (p *IntegrationRecordConsumer) Shutdown() {}

func TestCheckpointRecovery(t *testing.T) {
	rc := &IntegrationRecordConsumer{
		processedRecords: make(map[string]int),
	}
	kc := &KinesisConsumer{
		StreamName:           "checkpoint_recovery",
		ShardIteratorType:    "TRIM_HORIZON",
		RecordConsumer:       rc,
		TableName:            "checkpoint_recovery",
		EmptyRecordBackoffMs: 2000,
		LeaseDuration:        1,
	}
	pushRecordToKinesis("checkpoint_recovery", []byte("abcd"), true)

	err := kc.StartConsumer()
	if err != nil {
		t.Errorf("Error starting consumer %s", err)
	}

	kc.Shutdown()
	time.Sleep(200 * time.Millisecond)

	kc = &KinesisConsumer{
		StreamName:        "checkpoint_recovery",
		ShardIteratorType: "TRIM_HORIZON",
		RecordConsumer:    rc,
		TableName:         "checkpoint_recovery",
		LeaseDuration:     1,
	}

	err = kc.StartConsumer()
	if err != nil {
		t.Errorf("Error starting consumer %s", err)
	}
	time.Sleep(200 * time.Millisecond)
	for sequenceID, timesSequenceProcessed := range rc.processedRecords {
		fmt.Printf("seqenceID: %s, processed %d time(s)\n", sequenceID, timesSequenceProcessed)
		if timesSequenceProcessed > 1 {
			t.Errorf("Sequence number %s was processed more than once", sequenceID)
		}
	}
	kc.Shutdown()
}

func TestCheckpointGainLock(t *testing.T) {
	rc := &IntegrationRecordConsumer{
		processedRecords: make(map[string]int),
	}
	kc := &KinesisConsumer{
		StreamName:           "checkpoint_gain_lock",
		ShardIteratorType:    "TRIM_HORIZON",
		RecordConsumer:       rc,
		TableName:            "checkpoint_gain_lock",
		EmptyRecordBackoffMs: 2000,
		LeaseDuration:        100,
	}
	pushRecordToKinesis("checkpoint_gain_lock", []byte("abcd"), true)

	err := kc.StartConsumer()
	if err != nil {
		t.Errorf("Error starting consumer %s", err)
	}

	kc.Shutdown()
	time.Sleep(200 * time.Millisecond)

	kc = &KinesisConsumer{
		StreamName:        "checkpoint_gain_lock",
		ShardIteratorType: "TRIM_HORIZON",
		RecordConsumer:    rc,
		TableName:         "checkpoint_gain_lock",
		LeaseDuration:     100,
	}

	err = kc.StartConsumer()
	if err != nil {
		t.Errorf("Error starting consumer %s", err)
	}
	pushRecordToKinesis("checkpoint_gain_lock", []byte("abcd"), false)
	time.Sleep(200 * time.Millisecond)
	if len(rc.processedRecords) != 2 {
		t.Errorf("Expected to have processed 2 records")
		for sequenceId, timesProcessed := range rc.processedRecords {
			fmt.Println("Processed", sequenceId, timesProcessed, "time(s)")
		}
	}
	kc.Shutdown()
}

func TestPrometheusMonitoring(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	rc := &IntegrationRecordConsumer{
		processedRecords: make(map[string]int),
	}
	kc := &KinesisConsumer{
		StreamName:           "prometheus_monitoring",
		ShardIteratorType:    "TRIM_HORIZON",
		RecordConsumer:       rc,
		TableName:            "prometheus_monitoring",
		EmptyRecordBackoffMs: 2000,
		LeaseDuration:        1,
		Monitoring: MonitoringConfiguration{
			MonitoringService: "prometheus",
			Prometheus: prometheusMonitoringService{
				ListenAddress: ":8080",
			},
		},
	}
	pushRecordToKinesis("prometheus_monitoring", []byte("abcd"), true)

	err := kc.StartConsumer()
	if err != nil {
		t.Errorf("Error starting consumer %s", err)
	}
	time.Sleep(300 * time.Millisecond)

	res, err := http.Get("http://localhost:8080/metrics")
	if err != nil {
		t.Fatalf("Error scraping Prometheus endpoint %s", err)
	}
	kc.Shutdown()

	var parser expfmt.TextParser
	parsed, err := parser.TextToMetricFamilies(res.Body)
	res.Body.Close()
	if err != nil {
		t.Errorf("Error reading monitoring response %s", err)
	}

	if *parsed["gokini_processed_bytes"].Metric[0].Counter.Value != float64(4) {
		t.Errorf("Expected to have read 4 bytes, got %d", int(*parsed["gokini_processed_bytes"].Metric[0].Counter.Value))
	}

	if *parsed["gokini_processed_records"].Metric[0].Counter.Value != float64(1) {
		t.Errorf("Expected to have read 1 records, got %d", int(*parsed["gokini_processed_records"].Metric[0].Counter.Value))
	}

	if *parsed["gokini_leases_held"].Metric[0].Gauge.Value != float64(1) {
		t.Errorf("Expected to have 1 lease held, got %d", int(*parsed["gokini_leases_held"].Metric[0].Counter.Value))
	}
}
