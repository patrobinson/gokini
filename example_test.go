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
		err := consumer.CheckpointSequence(p.shardID, &records[len(records)-1].SequenceNumber)
		if err != nil {
			fmt.Printf("Error checkpointing sequence\n")
		}
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
	pushRecordToKinesis("KINESIS_STREAM", []byte("foo"))

	go func() {
		err := kc.StartConsumer()
		if err != nil {
			fmt.Printf("Failed to start consumer: %s", err)
		}
	}()

	// Wait for it to do it's thing
	time.Sleep(1 * time.Second)
	kc.Shutdown()

	// Output:
	// Checkpointer initializing
	// foo
	// PrintRecordConsumer Shutdown
}
