package gokini

import (
	"strings"
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
	checkpointFound    bool
	checkpoint         map[string]*shardStatus
	checkpointerCalled bool
}

func (c *mockCheckpointer) Init() error {
	c.checkpoint = make(map[string]*shardStatus)
	return nil
}

func (c *mockCheckpointer) GetLease(shard *shardStatus, assignTo string) error {
	shard.Lock()
	shard.AssignedTo = assignTo
	shard.LeaseTimeout = time.Now()
	shard.Unlock()
	return nil
}

func (c *mockCheckpointer) CheckpointSequence(shard *shardStatus) error {
	c.checkpoint[shard.ID] = shard
	c.checkpointerCalled = true
	return nil
}
func (c *mockCheckpointer) FetchCheckpoint(shard *shardStatus) error {
	if c.checkpointFound {
		if checkpointShard, ok := c.checkpoint[shard.ID]; ok {
			shard.Checkpoint = checkpointShard.Checkpoint
			shard.AssignedTo = checkpointShard.AssignedTo
		} else {
			shard.Checkpoint = "ABCD124"
			shard.AssignedTo = "abcdef-1234567"
		}
	}
	return nil
}

func (c *mockCheckpointer) ListActiveWorkers() (map[string][]string, error) {
	return nil, nil
}

func (c *mockCheckpointer) ClaimShard(*shardStatus, string) error {
	return nil
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
				SequenceNumber: aws.String(strings.Join([]string{"0000", string(k.numberRecordsSent)}, "")),
			},
		},
	}, nil
}

func createConsumer(t *testing.T, numRecords int, checkpointFound bool) (consumer *testConsumer, kinesisSvc *mockKinesisClient, checkpointer *mockCheckpointer) {
	consumer = &testConsumer{}
	kinesisSvc = &mockKinesisClient{
		NumberRecordsBeforeClosing: numRecords,
		RecordData:                 []byte("Hello World"),
	}
	checkpointer = &mockCheckpointer{
		checkpointFound: checkpointFound,
	}
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
	time.Sleep(200 * time.Millisecond)
	kc.Shutdown()
	return
}

func TestStartConsumer(t *testing.T) {
	consumer, kinesisSvc, _ := createConsumer(t, 1, false)

	if consumer.ShardID != "00000001" {
		t.Errorf("Expected shardId to be set to 00000001, but got: %s", consumer.ShardID)
	}

	if len(consumer.Records) != 1 {
		t.Errorf("Expected there to be one record from Kinesis, got %d", len(consumer.Records))
	} else if string(consumer.Records[0].Data) != "Hello World" {
		t.Errorf("Expected record to be \"Hello World\", got %s", consumer.Records[1].Data)
	}

	time.Sleep(200 * time.Millisecond)
	if consumer.IsShutdown != true {
		t.Errorf("Expected consumer to be shutdown but it was not")
	}

	consumer, kinesisSvc, _ = createConsumer(t, 2, true)
	if len(consumer.Records) != 2 {
		t.Errorf("Expected there to be two records from Kinesis, got %v", consumer.Records)
	}

	if !kinesisSvc.getShardIteratorCalled {
		t.Errorf("Expected shard iterator to be called, but it was not")
	}

	if consumer.IsShutdown != true {
		t.Errorf("Expected consumer to be shutdown but it was not")
	}

	consumer, kinesisSvc, _ = createConsumer(t, 1, true)
	if !kinesisSvc.getShardIteratorCalled {
		t.Errorf("Expected shard iterator not to be called, but it was")
	}

}
