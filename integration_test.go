//+build integration

package gokini

import (
	"fmt"
	"testing"
	"time"
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
	pushRecordToKinesis("checkpoint_recovery", []byte("abcd"))

	err := kc.StartConsumer()
	if err != nil {
		t.Errorf("Error starting consumer %s", err)
	}

	kc.Shutdown()
	time.Sleep(1 * time.Second)

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
	time.Sleep(1 * time.Second)
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
		LeaseDuration:        1,
	}
	pushRecordToKinesis("checkpoint_gain_lock", []byte("abcd"))

	err := kc.StartConsumer()
	if err != nil {
		t.Errorf("Error starting consumer %s", err)
	}

	kc.Shutdown()
	time.Sleep(1 * time.Second)

	kc = &KinesisConsumer{
		StreamName:        "checkpoint_gain_lock",
		ShardIteratorType: "TRIM_HORIZON",
		RecordConsumer:    rc,
		TableName:         "checkpoint_gain_lock",
		LeaseDuration:     1,
	}

	err = kc.StartConsumer()
	if err != nil {
		t.Errorf("Error starting consumer %s", err)
	}
	pushRecordToKinesis("checkpoint_gain_lock", []byte("abcd"))
	time.Sleep(1 * time.Second)
	if len(rc.processedRecords) != 2 {
		t.Errorf("Expected to have processed 2 records")
		for sequenceId, timesProcessed := range rc.processedRecords {
			fmt.Println("Processed", sequenceId, timesProcessed, "time(s)")
		}
	}
	kc.Shutdown()
}
