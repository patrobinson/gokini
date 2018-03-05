//+build integration

package gokini

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
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
		consumer.CheckpointSequence(p.shardID, &records[len(records)-1].SequenceNumber)
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
		EmptyRecordBackoffMs: 1000,
	}
	pushRecordToKinesis("checkpoint_recovery", []byte("abcd"))

	go startConsumer(kc, t)

	time.Sleep(1 * time.Second)
	kc.Shutdown()

	kc = &KinesisConsumer{
		StreamName:        "checkpoint_recovery",
		ShardIteratorType: "TRIM_HORIZON",
		RecordConsumer:    rc,
		TableName:         "checkpoint_recovery",
	}

	go startConsumer(kc, t)
	time.Sleep(1 * time.Second)
	for sequenceID, timesSequenceProcessed := range rc.processedRecords {
		fmt.Printf("seqenceID: %s, processed %d time(s)\n", sequenceID, timesSequenceProcessed)
		if timesSequenceProcessed > 1 {
			t.Errorf("Sequence number %s was processed more than once", sequenceID)
		}
	}
}

func startConsumer(kc *KinesisConsumer, t *testing.T) {
	err := kc.StartConsumer()
	if err != nil {
		t.Fatalf("Failed to start consumer: %s", err)
	}
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
