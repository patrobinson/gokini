package gokini

import (
	"fmt"
	"time"
)

type CheckpointRecordConsumer struct {
	shardID string
}

func (p *CheckpointRecordConsumer) Init(shardID string) error {
	fmt.Printf("Checkpointer initializing\n")
	p.shardID = shardID
	return nil
}

func (p *CheckpointRecordConsumer) ProcessRecords(records []*Records, consumer *KinesisConsumer) {
	if len(records) > 0 {
		fmt.Printf("%s\n", records[0].Data)
	}
	consumer.Checkpoint(p.shardID, records[len(records)-1].SequenceNumber)
}

func (p *CheckpointRecordConsumer) Shutdown() {
	fmt.Print("PrintRecordConsumer Shutdown\n")
}

func ExampleCheckpointRecordConsumer() {
	// An implementation of the RecordConsumer interface that prints out records and checkpoints at the end
	rc := &PrintRecordConsumer{}
	kc := &KinesisConsumer{
		StreamName:           "KINESIS_STREAM_2",
		ShardIteratorType:    "TRIM_HORIZON",
		RecordConsumer:       rc,
		TableName:            "gokini_2",
		EmptyRecordBackoffMs: 1000,
	}

	// Send records to our kinesis stream so we have something to process
	pushRecordToKinesis("KINESIS_STREAM_2", []byte("example_checkpoint_record_consumer"), true)

	err := kc.StartConsumer()
	if err != nil {
		fmt.Printf("Failed to start consumer: %s", err)
	}

	// Wait for it to do it's thing
	time.Sleep(200 * time.Millisecond)
	kc.Shutdown()

	// Output:
	// Checkpointer initializing
	// example_checkpoint_record_consumer
	// PrintRecordConsumer Shutdown
}
