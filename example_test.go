package gokini

import (
	"fmt"
	"time"
)

type PrintRecordConsumer struct {
	shardID string
}

func (p *PrintRecordConsumer) Init(shardID string) error {
	fmt.Printf("Checkpointer initializing\n")
	p.shardID = shardID
	return nil
}

func (p *PrintRecordConsumer) ProcessRecords(records []*Records, consumer *KinesisConsumer) {
	if len(records) > 0 {
		fmt.Printf("%s\n", records[0].Data)
	}
}

func (p *PrintRecordConsumer) Shutdown() {
	fmt.Print("PrintRecordConsumer Shutdown\n")
}

func ExampleRecordConsumer() {
	// An implementation of the RecordConsumer interface that prints out records
	rc := &PrintRecordConsumer{}
	kc := &KinesisConsumer{
		StreamName:           "KINESIS_STREAM",
		ShardIteratorType:    "TRIM_HORIZON",
		RecordConsumer:       rc,
		TableName:            "gokini",
		EmptyRecordBackoffMs: 1000,
	}

	// Send records to our kinesis stream so we have something to process
	pushRecordToKinesis("KINESIS_STREAM", []byte("foo"), true)

	err := kc.StartConsumer()
	if err != nil {
		fmt.Printf("Failed to start consumer: %s", err)
	}

	// Wait for it to do it's thing
	time.Sleep(200 * time.Millisecond)
	kc.Shutdown()

	// Output:
	// Checkpointer initializing
	// foo
	// PrintRecordConsumer Shutdown
}
