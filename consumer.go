package gokini

import (
	"errors"

	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

// RecordConsumer is the interface consumers will implement
type RecordConsumer interface {
	Init(string) error
	ProcessRecords([]*Records, Checkpointer)
	Shutdown()
}

// Records is structure for Kinesis Records
type Records struct {
	Data           []byte `json:"data"`
	PartitionKey   string `json:"partitionKey"`
	SequenceNumber string `json:"sequenceNumber"`
}

type shardStatus struct {
	ShardID    string
	Checkpoint string
}

// KinesisConsumer contains all the configuration and functions necessary to start the Kinesis Consumer
type KinesisConsumer struct {
	StreamName        string
	ShardIteratorType string
	RecordConsumer    RecordConsumer
	TableName         string
	svc               kinesisiface.KinesisAPI
	checkpointer      Checkpointer
	stop              *chan struct{}
}

// StartConsumer starts the RecordConsumer, calls Init and starts sending records to ProcessRecords
func (kc *KinesisConsumer) StartConsumer() error {
	if kc.svc == nil && kc.checkpointer == nil {
		session, err := session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable})
		if err != nil {
			return err
		}
		kc.svc = kinesis.New(session)
		kc.checkpointer = &DynamoCheckpoint{
			TableName: kc.TableName,
		}
	}
	return kc.startKinesisConsumer()
}

// Shutdown stops consuming records gracefully
func (kc *KinesisConsumer) Shutdown() {
	close(*kc.stop)
}

func (kc *KinesisConsumer) startKinesisConsumer() error {
	stopChan := make(chan struct{})
	kc.stop = &stopChan

	shards, err := getShardIDs(kc.svc, kc.StreamName, "")
	if err != nil {
		return err
	}
	for shardID := range shards {
		kc.RecordConsumer.Init(shardID)
		go kc.getRecords(shardID)
	}

	return nil
}

func getShardIDs(kinesisSvc kinesisiface.KinesisAPI, streamName string, startShardID string) (map[string]shardStatus, error) {
	shards := map[string]shardStatus{}
	args := &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	}
	streamDesc, err := kinesisSvc.DescribeStream(args)
	if err != nil {
		return shards, err
	}

	if *streamDesc.StreamDescription.StreamStatus != "ACTIVE" {
		return shards, errors.New("Stream not active")
	}

	var lastShardID string
	for _, s := range streamDesc.StreamDescription.Shards {
		shards[*s.ShardId] = shardStatus{
			ShardID: *s.ShardId,
		}
		lastShardID = *s.ShardId
	}

	if *streamDesc.StreamDescription.HasMoreShards {
		moreShards, err := getShardIDs(kinesisSvc, streamName, lastShardID)
		if err != nil {
			return shards, err
		}

		for k, v := range moreShards {
			shards[k] = v
		}
	}

	return shards, nil
}

func (kc *KinesisConsumer) getShardIterator(shardID string) (*string, error) {
	shardIterator, err := kc.checkpointer.FetchCheckpoint(shardID)
	if err != nil {
		if err != ErrSequenceIDNotFound {
			return nil, err
		}
		shardIterArgs := &kinesis.GetShardIteratorInput{
			ShardId:           aws.String(shardID),
			ShardIteratorType: aws.String(kc.ShardIteratorType),
			StreamName:        aws.String(kc.StreamName),
		}
		iterResp, err := kc.svc.GetShardIterator(shardIterArgs)
		if err != nil {
			return nil, err
		}
		return iterResp.ShardIterator, nil
	}

	return shardIterator, nil
}

func (kc *KinesisConsumer) getRecords(shardID string) {
	shardIterator, err := kc.getShardIterator(shardID)
	if err != nil {
		log.Fatalf("Unable to get shard iterator for %s: %s", shardID, err)
	}

	for {
		getRecordsArgs := &kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
		}
		getResp, err := kc.svc.GetRecords(getRecordsArgs)
		if err != nil {
			log.Errorf("Error getting records from shard %v: %v", shardID, err)
			continue
		}

		var records []*Records
		for _, r := range getResp.Records {
			record := &Records{
				Data:           r.Data,
				PartitionKey:   *r.PartitionKey,
				SequenceNumber: *r.SequenceNumber,
			}
			records = append(records, record)
		}
		kc.RecordConsumer.ProcessRecords(records, kc.checkpointer)

		// The shard has been closed, so no new records can be read from it
		if getResp.NextShardIterator == nil {
			kc.RecordConsumer.Shutdown()
			return
		}

		select {
		case <-*kc.stop:
			kc.RecordConsumer.Shutdown()
			return
		}
	}
}
