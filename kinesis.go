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
	ProcessRecords([]*Records, *Checkpointer)
	Shutdown()
}

// Records is structure for Kinesis Records
type Records struct {
	Data           []byte `json:"data"`
	PartitionKey   string `json:"partitionKey"`
	SequenceNumber string `json:"sequenceNumber"`
}

// Checkpointer handles checkpointing when a record has been processed
type Checkpointer struct {
}

// CheckpointAll writes a checkpoint at a pointer to the last record sent to ProcessRecords
func (checkpointer *Checkpointer) CheckpointAll() error {
	return nil
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
	svc               kinesisiface.KinesisAPI
	checkpointer      *Checkpointer
	stop              *chan struct{}
}

// StartConsumer starts the RecordConsumer, calls Init and starts sending records to ProcessRecords
func (kc *KinesisConsumer) StartConsumer() error {
	session, err := session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable})
	if err != nil {
		return err
	}
	kc.svc = kinesis.New(session)
	kc.checkpointer = &Checkpointer{}
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

func (kc *KinesisConsumer) getRecords(shardID string) {
	shardIterArgs := &kinesis.GetShardIteratorInput{
		ShardId:           aws.String(shardID),
		ShardIteratorType: aws.String(kc.ShardIteratorType),
		StreamName:        aws.String(kc.StreamName),
	}
	iterResp, err := kc.svc.GetShardIterator(shardIterArgs)
	if err != nil {
		log.Fatalf("Unable to retrieve records: %v", err)
	}

	for {
		getRecordsArgs := &kinesis.GetRecordsInput{
			ShardIterator: iterResp.ShardIterator,
		}
		getResp, err := kc.svc.GetRecords(getRecordsArgs)
		if err != nil {
			log.Errorf("Error getting records from shard %v: %v", shardID, err)
			continue
		}
		if getResp.NextShardIterator == nil {
			kc.RecordConsumer.Shutdown()
			return
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
		select {
		case <-*kc.stop:
			kc.RecordConsumer.Shutdown()
			return
		}
	}
}
