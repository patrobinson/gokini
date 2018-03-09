package gokini

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
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
}

func (k *mockKinesisClient) GetShardIterator(args *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	k.getShardIteratorCalled = true
	return &kinesis.GetShardIteratorOutput{
		ShardIterator: aws.String("0123456789ABCDEF"),
	}, nil
}

func (k *mockKinesisClient) DescribeStream(args *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
	return &kinesis.DescribeStreamOutput{
		StreamDescription: &kinesis.StreamDescription{
			StreamStatus: aws.String("ACTIVE"),
			Shards: []*kinesis.Shard{
				&kinesis.Shard{
					ShardId: aws.String("00000001"),
				},
			},
			HasMoreShards: aws.Bool(false),
		},
	}, nil
}

type mockCheckpointer struct {
	checkpointFound bool
}

func (c *mockCheckpointer) Init() error {
	return nil
}

func (c *mockCheckpointer) CheckpointSequence(string, *string, string) error {
	return nil
}
func (c *mockCheckpointer) FetchCheckpoint(string) (*string, error) {
	if c.checkpointFound {
		return aws.String("0123456789ABCDEF"), nil
	}
	return nil, ErrSequenceIDNotFound
}

func (k *mockKinesisClient) GetRecords(args *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	k.numberRecordsSent = k.numberRecordsSent + 1
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
				SequenceNumber: aws.String("012345"),
			},
		},
	}, nil
}

func TestStartConsumer(t *testing.T) {
	consumer := &testConsumer{}
	kinesisSvc := &mockKinesisClient{
		NumberRecordsBeforeClosing: 1,
		RecordData:                 []byte("Hello World"),
	}
	checkpointer := &mockCheckpointer{}
	kc := &KinesisConsumer{
		StreamName:        "FOO",
		ShardIteratorType: "TRIM_HORIZON",
		RecordConsumer:    consumer,
		checkpointer:      checkpointer,
		svc:               kinesisSvc,
	}

	err := kc.StartConsumer()
	if err != nil {
		t.Fatalf("Got unexpected error from StartConsumer: %s", err)
	}
	kc.Shutdown()
	if consumer.ShardID != "00000001" {
		t.Errorf("Expected shardId to be set to 00000001, but got: %s", consumer.ShardID)
	}

	if len(consumer.Records) != 1 {
		t.Errorf("Expected there to be one record from Kinesis, got %d", len(consumer.Records))
	} else if string(consumer.Records[0].Data) != "Hello World" {
		t.Errorf("Expected record to be \"Hello World\", got %s", consumer.Records[1].Data)
	}

	time.Sleep(1 * time.Second)
	if consumer.IsShutdown != true {
		t.Errorf("Expected consumer to be shutdown but it was not")
	}

	consumer = &testConsumer{}
	kinesisSvc = &mockKinesisClient{
		NumberRecordsBeforeClosing: 2,
		RecordData:                 []byte("Hello World"),
	}
	kc = &KinesisConsumer{
		StreamName:        "FOO",
		ShardIteratorType: "TRIM_HORIZON",
		RecordConsumer:    consumer,
		checkpointer:      checkpointer,
		svc:               kinesisSvc,
	}
	err = kc.StartConsumer()
	if err != nil {
		t.Fatalf("Got unexpected error from StartConsumer: %s", err)
	}
	time.Sleep(1 * time.Second)
	kc.Shutdown()
	if len(consumer.Records) != 2 {
		t.Errorf("Expected there to be two records from Kinesis, got %d", len(consumer.Records))
	}

	if !kinesisSvc.getShardIteratorCalled {
		t.Errorf("Expected shard iterator to be called, but it was not")
	}

	if consumer.IsShutdown != true {
		t.Errorf("Expected consumer to be shutdown but it was not")
	}

	consumer = &testConsumer{}
	kinesisSvc = &mockKinesisClient{
		NumberRecordsBeforeClosing: 1,
		RecordData:                 []byte("Hello World"),
	}
	checkpointer = &mockCheckpointer{
		checkpointFound: true,
	}
	kc = &KinesisConsumer{
		StreamName:        "FOO",
		ShardIteratorType: "TRIM_HORIZON",
		RecordConsumer:    consumer,
		checkpointer:      checkpointer,
		svc:               kinesisSvc,
	}
	err = kc.StartConsumer()
	if err != nil {
		t.Fatalf("Got unexpected error from StartConsumer: %s", err)
	}
	if kinesisSvc.getShardIteratorCalled {
		t.Errorf("Expected shard iterator not to be called, but it was")
	}
	time.Sleep(1 * time.Second)
	kc.Shutdown()
}
