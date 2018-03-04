//+build integration

package gokini

import (
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	log "github.com/sirupsen/logrus"
)

type PrintRecordConsumer struct {
	shardID string
}

func (p *PrintRecordConsumer) Init(shardID string) error {
	fmt.Printf("Checkpointer initializing\n")
	p.shardID = shardID
	return nil
}

func (p *PrintRecordConsumer) ProcessRecords(records []*Records, checkpointer Checkpointer) {
	if len(records) > 0 {
		fmt.Printf("%s\n", records[0].Data)
		err := checkpointer.CheckpointSequence(p.shardID, &records[len(records)-1].SequenceNumber)
		if err != nil {
			fmt.Printf("Error checkpointing sequence\n")
		}
	}
}

func (p *PrintRecordConsumer) Shutdown() {
	fmt.Print("PrintRecordConsumer Shutdown\n")
}

func ExampleRecordConsumer() {
	log.SetLevel(log.DebugLevel)

	// An implementation of the RecordConsumer interface that prints out records
	rc := &PrintRecordConsumer{}
	kc := &KinesisConsumer{
		StreamName:        "KINESIS_STREAM",
		ShardIteratorType: "TRIM_HORIZON",
		RecordConsumer:    rc,
		TableName:         "gokini",
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

func pushRecordToKinesis(streamName string, record []byte) error {
	session, err := session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable})
	if err != nil {
		log.Errorf("Error starting kinesis client %s", err)
		return err
	}
	session.Config.Endpoint = aws.String(os.Getenv("KINESIS_ENDPOINT"))
	svc := kinesis.New(session)
	_, err = svc.CreateStream(&kinesis.CreateStreamInput{
		ShardCount: aws.Int64(1),
		StreamName: aws.String(streamName),
	})
	if err != nil {
		log.Errorf("Error creating kinesis stream %s", err)
		return err
	}
	time.Sleep(500 * time.Millisecond)
	_, err = svc.PutRecord(&kinesis.PutRecordInput{
		Data:         record,
		PartitionKey: aws.String("abc123"),
		StreamName:   &streamName,
	})
	if err != nil {
		log.Errorf("Error sending data to kinesis %s", err)
	}
	return err
}
