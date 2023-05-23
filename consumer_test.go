package gokini

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/mixer/clock"
)

type testConsumer struct {
	ShardID    string
	Records    []*Records
	IsShutdown bool
}

func (tc *testConsumer) Init(shardID string) error {
	tc.ShardID = shardID
	return nil
}

func (tc *testConsumer) ProcessRecords(records []*Records, consumer *KinesisConsumer) {
	tc.Records = append(tc.Records, records...)
}

func (tc *testConsumer) Shutdown() {
	tc.IsShutdown = true
	return
}

type mockKinesisClient struct {
	kinesisiface.KinesisAPI
	NumberRecordsBeforeClosing int
	numberRecordsSent          int
	getShardIteratorCalled     bool
	RecordData                 []byte
	numShards                  int
}

func (k *mockKinesisClient) GetShardIterator(args *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	k.getShardIteratorCalled = true
	return &kinesis.GetShardIteratorOutput{
		ShardIterator: aws.String("0123456789ABCDEF"),
	}, nil
}

func (k *mockKinesisClient) DescribeStream(args *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
	shards := []*kinesis.Shard{}
	for i := 0; i < k.numShards; i++ {
		shards = append(shards, &kinesis.Shard{
			ShardId: aws.String(fmt.Sprintf("0000000%d", i)),
		})
	}
	return &kinesis.DescribeStreamOutput{
		StreamDescription: &kinesis.StreamDescription{
			StreamStatus:  aws.String("ACTIVE"),
			Shards:        shards,
			HasMoreShards: aws.Bool(false),
		},
	}, nil
}

func (k *mockKinesisClient) GetRecords(args *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	k.numberRecordsSent++
	var nextShardIterator *string
	if k.NumberRecordsBeforeClosing == 0 || k.numberRecordsSent < k.NumberRecordsBeforeClosing {
		nextShardIterator = aws.String("ABCD1234")
	}
	return &kinesis.GetRecordsOutput{
		MillisBehindLatest: aws.Int64(0),
		NextShardIterator:  nextShardIterator,
		Records: []*kinesis.Record{
			&kinesis.Record{
				Data:           k.RecordData,
				PartitionKey:   aws.String("abcdefg"),
				SequenceNumber: aws.String(strings.Join([]string{"0000", fmt.Sprint(k.numberRecordsSent)}, "")),
			},
		},
	}, nil
}

func createConsumer(t *testing.T, numRecords int, checkpointFound bool, shutdown bool) (consumer *testConsumer, kinesisSvc *mockKinesisClient, checkpointer *DynamoCheckpoint) {
	consumer = &testConsumer{}
	kinesisSvc = &mockKinesisClient{
		NumberRecordsBeforeClosing: numRecords,
		RecordData:                 []byte("Hello World"),
		numShards:                  1,
	}
	date, _ := time.Parse(time.UnixDate, "Sat Mar  7 11:12:39 PST 2015")
	c := clock.NewMockClock(date)
	checkpointer = &DynamoCheckpoint{
		TableName: "TableName",
		Session:   session.New(),
	}
	checkpointer.Init()
	kc := &KinesisConsumer{
		StreamName:        "FOO",
		ShardIteratorType: "TRIM_HORIZON",
		RecordConsumer:    consumer,
		checkpointer:      checkpointer,
		svc:               kinesisSvc,
		eventLoopSleepMs:  1,
		Clock:             c,
	}

	err := kc.StartConsumer()
	if err != nil {
		t.Fatalf("Got unexpected error from StartConsumer: %s", err)
	}
	time.Sleep(200 * time.Millisecond)
	if shutdown {
		kc.Shutdown()
	}
	return
}

func TestStartConsumer(t *testing.T) {
	consumer, kinesisSvc, _ := createConsumer(t, 1, false, true)

	if consumer.ShardID != "00000000" {
		t.Errorf("Expected shardId to be set to 00000000, but got: %s", consumer.ShardID)
	}

	if len(consumer.Records) != 1 {
		t.Fatalf("Expected there to be one record from Kinesis, got %d", len(consumer.Records))
	}

	if string(consumer.Records[0].Data) != "Hello World" {
		t.Errorf("Expected record to be \"Hello World\", got %s", consumer.Records[0].Data)
	}

	if string(consumer.Records[0].ShardID) != "00000000" {
		t.Errorf("Expected Shard ID to be \"00000000\", got %s", consumer.Records[0].ShardID)
	}

	if !consumer.IsShutdown {
		t.Errorf("Expected consumer to be shutdown but it was not")
	}

	consumer, kinesisSvc, _ = createConsumer(t, 2, true, true)
	if len(consumer.Records) != 2 {
		t.Errorf("Expected there to be two records from Kinesis, got %v", consumer.Records)
	}

	if !kinesisSvc.getShardIteratorCalled {
		t.Errorf("Expected shard iterator to be called, but it was not")
	}

	if !consumer.IsShutdown {
		t.Errorf("Expected consumer to be shutdown but it was not")
	}

	consumer, kinesisSvc, _ = createConsumer(t, 1, true, true)
	if !kinesisSvc.getShardIteratorCalled {
		t.Errorf("Expected shard iterator not to be called, but it was")
	}
}

func TestScaleDownShards(t *testing.T) {
	consumer, kinesisSvc, _ := createConsumer(t, 0, false, false)
	kinesisSvc.numShards = 2
	time.Sleep(10 * time.Millisecond)
	// Shards don't just "dissapear" they rather just return a nil nextIteratorShard
	kinesisSvc.NumberRecordsBeforeClosing = 1
	time.Sleep(10 * time.Millisecond)
	if consumer.IsShutdown {
		t.Errorf("Expected consumer to not be shutdown but it was")
	}
}
