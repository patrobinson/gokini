package gokini

import (
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
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
	ID         string
	Checkpoint string
	AssignedTo string
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
	shardStatus       map[string]*shardStatus
	consumerID        string
	sigs              *chan os.Signal
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

	if err := kc.checkpointer.Init(); err != nil {
		log.Fatalf("Failed to start Checkpointer: %s", err)
	}
	kc.shardStatus = make(map[string]*shardStatus)

	sigs := make(chan os.Signal, 1)
	kc.sigs = &sigs
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	stopChan := make(chan struct{})
	kc.stop = &stopChan

	kc.consumerID = uuid.New().String()

	for {
		var err error
		err = kc.getShardIDs("")
		if err != nil {
			return err
		}

		for _, shard := range kc.shardStatus {
			if shard.AssignedTo == kc.consumerID {
				continue
			}

			kc.RecordConsumer.Init(shard.ID)
			shard.AssignedTo = kc.consumerID
			log.Debugf("Starting consumer for shard %s on %s", shard.ID, shard.AssignedTo)
			go kc.getRecords(shard.ID)
		}
		select {
		case sig := <-sigs:
			log.Infof("Received signal %s. Exiting", sig)
			kc.Shutdown()
		case <-time.After(1 * time.Second):
		}
	}
}

// Shutdown stops consuming records gracefully
func (kc *KinesisConsumer) Shutdown() {
	close(*kc.stop)
	time.Sleep(1 * time.Second)
}

func (kc *KinesisConsumer) getShardIDs(startShardID string) error {
	args := &kinesis.DescribeStreamInput{
		ExclusiveStartShardId: aws.String(startShardID),
		StreamName:            aws.String(kc.StreamName),
	}
	streamDesc, err := kc.svc.DescribeStream(args)
	if err != nil {
		return err
	}

	if *streamDesc.StreamDescription.StreamStatus != "ACTIVE" {
		return errors.New("Stream not active")
	}

	var lastShardID string
	for _, s := range streamDesc.StreamDescription.Shards {
		if _, ok := kc.shardStatus[*s.ShardId]; !ok {
			kc.shardStatus[*s.ShardId] = &shardStatus{
				ID: *s.ShardId,
			}
		}
		lastShardID = *s.ShardId
	}

	if *streamDesc.StreamDescription.HasMoreShards {
		err := kc.getShardIDs(lastShardID)
		if err != nil {
			return err
		}
	}

	return nil
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
			log.Debugf("Shard %s closed", shardID)
			kc.RecordConsumer.Shutdown()
			return
		}

		select {
		case <-*kc.stop:
			kc.RecordConsumer.Shutdown()
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}
