package gokini

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/google/uuid"
	"github.com/mixer/clock"
	log "github.com/sirupsen/logrus"
)

const (
	defaultEmptyRecordBackoffMs     = 500
	defaultMillisecondsBackoffClaim = 30000
	defaultEventLoopSleepMs         = 1000
	// ErrCodeKMSThrottlingException is defined in the API Reference https://docs.aws.amazon.com/sdk-for-go/api/service/kinesis/#Kinesis.GetRecords
	// But it's not a constant?
	ErrCodeKMSThrottlingException = "KMSThrottlingException"
)

// RecordConsumer is the interface consumers will implement
type RecordConsumer interface {
	Init(string) error
	ProcessRecords([]*Records, *KinesisConsumer)
	Shutdown()
}

// Records is structure for Kinesis Records
type Records struct {
	Data           []byte `json:"data"`
	PartitionKey   string `json:"partitionKey"`
	SequenceNumber string `json:"sequenceNumber"`
	ShardID        string `json:"shardID"`
}

// KinesisConsumer contains all the configuration and functions necessary to start the Kinesis Consumer
type KinesisConsumer struct {
	StreamName                  string
	ShardIteratorType           string
	RecordConsumer              RecordConsumer
	EmptyRecordBackoffMs        int
	LeaseDuration               int
	Monitoring                  MonitoringConfiguration
	DisableAutomaticCheckpoints bool
	Retries                     *int
	IgnoreShardOrdering         bool
	TableName                   string
	DynamoReadCapacityUnits     *int64
	DynamoWriteCapacityUnits    *int64
	DynamoBillingMode           *string
	Session                     *session.Session // Setting session means Retries is ignored
	millisecondsBackoffClaim    int
	eventLoopSleepMs            int
	svc                         kinesisiface.KinesisAPI
	checkpointer                *DynamoCheckpoint
	stop                        *chan struct{}
	shards                      map[string]*ShardStateMachine
	consumerID                  string
	mService                    monitoringService
	shardStealInProgress        bool
	Clock                       clock.Clock
	sync.WaitGroup
}

var defaultRetries = 5

// StartConsumer starts the RecordConsumer, calls Init and starts sending records to ProcessRecords
func (kc *KinesisConsumer) StartConsumer() error {
	kc.Clock = clock.C
	/*
	**	Initialisation
	 */
	rand.Seed(time.Now().UnixNano())

	// Set Defaults
	if kc.EmptyRecordBackoffMs == 0 {
		kc.EmptyRecordBackoffMs = defaultEmptyRecordBackoffMs
	}

	kc.consumerID = uuid.New().String()

	err := kc.Monitoring.init(kc.StreamName, kc.consumerID, kc.Session)
	if err != nil {
		log.Errorf("Failed to start monitoring service: %s", err)
	}
	kc.mService = kc.Monitoring.service

	if kc.millisecondsBackoffClaim == 0 {
		kc.millisecondsBackoffClaim = defaultMillisecondsBackoffClaim
	}

	if kc.eventLoopSleepMs == 0 {
		kc.eventLoopSleepMs = defaultEventLoopSleepMs
	}

	retries := defaultRetries
	if kc.Retries != nil {
		retries = *kc.Retries
	}

	if kc.Session == nil {
		log.Debugln("Creating AWS Session", kc.consumerID)
		kc.Session, err = session.NewSessionWithOptions(
			session.Options{
				Config:            aws.Config{Retryer: client.DefaultRetryer{NumMaxRetries: retries}},
				SharedConfigState: session.SharedConfigEnable,
			},
		)
		if err != nil {
			return err
		}
	}

	if kc.svc == nil && kc.checkpointer == nil {
		if endpoint := os.Getenv("KINESIS_ENDPOINT"); endpoint != "" {
			kc.Session.Config.Endpoint = aws.String(endpoint)
		}
		kc.svc = kinesis.New(kc.Session)
		kc.checkpointer = &DynamoCheckpoint{
			ReadCapacityUnits:  kc.DynamoReadCapacityUnits,
			WriteCapacityUnits: kc.DynamoWriteCapacityUnits,
			BillingMode:        kc.DynamoBillingMode,
			TableName:          kc.TableName,
			Retries:            retries,
			LeaseDuration:      kc.LeaseDuration,
			Session:            kc.Session,
		}
	}

	log.Debugf("Initializing Checkpointer")
	if err := kc.checkpointer.Init(); err != nil {
		return errors.Wrapf(err, "Failed to start Checkpointer")
	}

	kc.shards = make(map[string]*ShardStateMachine)

	stopChan := make(chan struct{})
	kc.stop = &stopChan

	err = kc.getShardIDs("")
	if err != nil {
		log.Errorf("Error getting Kinesis shards: %s", err)
		return err
	}
	go kc.eventLoop()

	return nil
}

func (kc *KinesisConsumer) eventLoop() {
	for {
		log.Debug("Getting shards")
		err := kc.getShardIDs("")
		if err != nil {
			log.Errorf("Error getting Kinesis shards: %s", err)
			// Back-off?
			kc.Clock.Sleep(500 * time.Millisecond)
			continue
		}
		log.Debugf("Found %d shards", len(kc.shards))

		for _, shard := range kc.shards {
			// We already own this shard so carry on
			if shard.AssignedTo() == kc.consumerID {
				continue
			}

			err = shard.GetLease(kc.consumerID)
			if err == nil {
				kc.mService.leaseGained(shard.ID())
				kc.RecordConsumer.Init(shard.ID())
				log.Debugf("Starting consumer for shard %s on %s", shard.ID(), shard.AssignedTo())
				kc.Add(1)
				go kc.getRecords(shard.ID())
				continue
			}
			if err.Error() != ErrLeaseNotAcquired {
				log.Error(err)
			}
		}

		err = kc.rebalance()
		if err != nil {
			log.Warn(err)
		}

		select {
		case <-*kc.stop:
			log.Info("Shutting down")
			return
		case <-kc.Clock.After(time.Duration(kc.eventLoopSleepMs) * time.Millisecond):
		}
	}
}

// Shutdown stops consuming records gracefully
func (kc *KinesisConsumer) Shutdown() {
	close(*kc.stop)
	kc.Wait()
	kc.RecordConsumer.Shutdown()
}

func (kc *KinesisConsumer) getShardIDs(startShardID string) error {
	args := &kinesis.DescribeStreamInput{
		StreamName: aws.String(kc.StreamName),
	}
	if startShardID != "" {
		args.ExclusiveStartShardId = aws.String(startShardID)
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
		if _, ok := kc.shards[*s.ShardId]; !ok {
			log.Debugf("Found shard with id %s", *s.ShardId)
			machine, err := NewShardStateMachine(s.ShardId, s.ParentShardId, kc.checkpointer)
			if err != nil {
				return err
			}
			kc.shards[*s.ShardId] = machine
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

func (kc *KinesisConsumer) getShardIterator(shard *ShardStateMachine) (*string, error) {
	err := shard.Sync(kc.checkpointer)
	if err != nil {
		return nil, err
	}

	if shard.Checkpoint() == "" {
		shardIterArgs := &kinesis.GetShardIteratorInput{
			ShardId:           aws.String(shard.ID()),
			ShardIteratorType: &kc.ShardIteratorType,
			StreamName:        &kc.StreamName,
		}
		iterResp, err := kc.svc.GetShardIterator(shardIterArgs)
		if err != nil {
			return nil, err
		}
		return iterResp.ShardIterator, nil
	}

	shardIterArgs := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(shard.ID()),
		ShardIteratorType:      aws.String("AFTER_SEQUENCE_NUMBER"),
		StartingSequenceNumber: aws.String(shard.Checkpoint()),
		StreamName:             &kc.StreamName,
	}
	iterResp, err := kc.svc.GetShardIterator(shardIterArgs)
	if err != nil {
		return nil, err
	}
	return iterResp.ShardIterator, nil
}

func (kc *KinesisConsumer) getRecords(shardID string) {
	defer kc.Done()

	shard := kc.shards[shardID]
	shardIterator, err := kc.getShardIterator(shard)
	if err != nil {
		kc.RecordConsumer.Shutdown()
		log.Errorf("Unable to get shard iterator for %s: %s", shardID, err)
		return
	}

	var retriedErrors int

	for {
		getRecordsStartTime := kc.Clock.Now()
		if kc.Clock.Now().UTC().After(shard.LeaseTimeout().Add(-5 * time.Second)) {
			err = shard.RenewLease(kc.consumerID)
			if err != nil {
				if err.Error() == ErrLeaseNotAcquired {
					kc.mService.leaseLost(shard.ID())
					log.Debugln("Lease lost for shard", shard.ID(), kc.consumerID)
					return
				}
				log.Warnln("Error renewing lease", err)
				kc.Clock.Sleep(time.Duration(1) * time.Second)
				continue
			}
		}

		getRecordsArgs := &kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
		}
		getResp, err := kc.svc.GetRecords(getRecordsArgs)
		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				if awsErr.Code() == kinesis.ErrCodeProvisionedThroughputExceededException || awsErr.Code() == ErrCodeKMSThrottlingException {
					log.Errorf("Error getting records from shard %v: %v", shardID, err)
					retriedErrors++
					kc.Clock.Sleep(time.Duration(2^retriedErrors*100) * time.Millisecond)
					continue
				}
			}
			// This is an exception we cannot handle and therefore we exit
			// Should probably do something other than panic here
			panic(fmt.Sprintf("Error getting records from Kinesis that cannot be retried: %s\nRequest: %s", err, getRecordsArgs))
		}
		retriedErrors = 0

		var records []*Records
		var recordBytes int64
		for _, r := range getResp.Records {
			record := &Records{
				Data:           r.Data,
				PartitionKey:   *r.PartitionKey,
				SequenceNumber: *r.SequenceNumber,
				ShardID:        shardID,
			}
			records = append(records, record)
			recordBytes += int64(len(record.Data))
			log.Tracef("Processing record %s", *r.SequenceNumber)
		}
		processRecordsStartTime := kc.Clock.Now()
		kc.RecordConsumer.ProcessRecords(records, kc)

		// Convert from nanoseconds to milliseconds
		processedRecordsTiming := kc.Clock.Since(processRecordsStartTime) / 1000000
		kc.mService.recordProcessRecordsTime(shard.ID(), float64(processedRecordsTiming))

		if len(records) == 0 {
			kc.Clock.Sleep(time.Duration(kc.EmptyRecordBackoffMs) * time.Millisecond)
		} else if !kc.DisableAutomaticCheckpoints {
			shard.CheckpointSequence(*getResp.Records[len(getResp.Records)-1].SequenceNumber, kc.consumerID)
		}

		kc.mService.incrRecordsProcessed(shard.ID(), len(records))
		kc.mService.incrBytesProcessed(shard.ID(), recordBytes)
		kc.mService.millisBehindLatest(shard.ID(), float64(*getResp.MillisBehindLatest))

		// Convert from nanoseconds to milliseconds
		getRecordsTime := kc.Clock.Since(getRecordsStartTime) / 1000000
		kc.mService.recordGetRecordsTime(shard.ID(), float64(getRecordsTime))

		// The shard has been closed, so no new records can be read from it
		if getResp.NextShardIterator == nil {
			log.Debugf("Shard %s closed", shardID)
			if !kc.DisableAutomaticCheckpoints {
				shard.Close(*getResp.Records[len(getResp.Records)-1].SequenceNumber, kc.consumerID)
			}
			return
		}
		shardIterator = getResp.NextShardIterator

		select {
		case <-*kc.stop:
			log.Infoln("Received stop signal, stopping record consumer for", shardID)
			return
		case <-kc.Clock.After(1 * time.Nanosecond):
		}
	}
}

func (kc *KinesisConsumer) Checkpoint(shardID string, sequenceNumber string) error {
	if shard, ok := kc.shards[shardID]; ok {
		return shard.CheckpointSequence(sequenceNumber, kc.consumerID)
	}
	return fmt.Errorf("Unknown shard ID %s", shardID)
}

func (kc *KinesisConsumer) rebalance() error {
	workers, err := kc.checkpointer.ListActiveWorkers()
	if err != nil {
		log.Debugln("Error listing workings", kc.consumerID, err)
		return err
	}

	var numShards float64
	for _, shards := range workers {
		numShards += float64(len(shards))
	}
	numWorkers := float64(len(workers))

	// 1:1 shards to workers is optimal, so we cannot possibly rebalance
	if numWorkers >= numShards {
		log.Debugln("Optimal shard allocation, not stealing any shards", numWorkers, ">", numShards, kc.consumerID)
		return nil
	}

	currentShards, ok := workers[kc.consumerID]
	var numCurrentShards float64
	if !ok {
		numCurrentShards = 0
		numWorkers++
	} else {
		numCurrentShards = float64(len(currentShards))
	}

	optimalShards := math.Floor(numShards / numWorkers)
	log.Debugln("Number of shards", numShards)
	log.Debugln("Number of workers", numWorkers)
	log.Debugln("Optimal shards", optimalShards)
	log.Debugln("Current shards", numCurrentShards)
	// We have more than or equal optimal shards, so no rebalancing can take place
	if numCurrentShards >= optimalShards {
		log.Debugln("We have enough shards, not attempting to steal any", kc.consumerID)
		return nil
	}
	maxShards := int(optimalShards)
	var workerSteal *string
	for w, shards := range workers {
		if len(shards) > maxShards {
			workerSteal = &w
			maxShards = len(shards)
		}
	}
	// Not all shards are allocated so fallback to default shard allocation mechanisms
	if workerSteal == nil {
		log.Debugln("Not all shards are allocated, not stealing any", kc.consumerID)
		return nil
	}

	// Steal a random shard from the worker with the most shards
	randIndex := rand.Perm(len(workers[*workerSteal]))[0]
	shardToSteal := workers[*workerSteal][randIndex]
	log.Debugln("Stealing shard", shardToSteal, "from", *workerSteal)
	err = kc.shards[shardToSteal].ClaimShard(kc.consumerID)
	return err
}
