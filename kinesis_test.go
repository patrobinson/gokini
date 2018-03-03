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

func (tc *testConsumer) ProcessRecords(records []*Records, checkpointer *Checkpointer) {
	tc.Records = append(tc.Records, records...)
	checkpointer.CheckpointAll()
}

func (tc *testConsumer) Shutdown() {
	tc.IsShutdown = true
	return
}

type mockKinesisClient struct {
	kinesisiface.KinesisAPI
}

func (k *mockKinesisClient) GetShardIterator(args *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
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

func (k *mockKinesisClient) GetRecords(args *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	return &kinesis.GetRecordsOutput{
		MillisBehindLatest: aws.Int64(0),
		NextShardIterator:  aws.String("ABCD1234"),
		Records: []*kinesis.Record{
			&kinesis.Record{
				Data:           []byte("Hello World"),
				PartitionKey:   aws.String("abcdefg"),
				SequenceNumber: aws.String("012345"),
			},
		},
	}, nil
}

func TestRecordConsumerInterface(t *testing.T) {
	consumer := &testConsumer{}
	kinesisSvc := &mockKinesisClient{}
	kc := &KinesisConsumer{
		StreamName:        "FOO",
		ShardIteratorType: "TRIM_HORIZON",
		RecordConsumer:    consumer,
		svc:               kinesisSvc,
	}
	kc.startKinesisConsumer()
	time.Sleep(time.Duration(1 * time.Second))
	kc.Shutdown()
	if consumer.ShardID != "00000001" {
		t.Errorf("Expected shardId to be set to 00000001, but got: %s", consumer.ShardID)
	}

	if len(consumer.Records) != 1 {
		t.Errorf("Expected there to be one record from Kinesis, got %d", len(consumer.Records))
	} else if string(consumer.Records[0].Data) != "Hello World" {
		t.Errorf("Expected record to be \"Hello World\", got %s", consumer.Records[1].Data)
	}

	time.Sleep(time.Duration(1 * time.Second))
	if consumer.IsShutdown != true {
		t.Errorf("Expected consumer to be shutdown but it was not")
	}
}
