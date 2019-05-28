package gokini

import (
	"errors"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	log "github.com/sirupsen/logrus"
)

const (
	defaultLeaseDuration = 30000
	// ErrLeaseNotAquired is returned when we failed to get a lock on the shard
	ErrLeaseNotAquired = "Lease is already held by another node"
	// ErrInvalidDynamoDBSchema is returned when there are one or more fields missing from the table
	ErrInvalidDynamoDBSchema = "The DynamoDB schema is invalid and may need to be re-created"
)

// Checkpointer handles checkpointing when a record has been processed
type Checkpointer interface {
	Init() error
	GetLease(*shardStatus, string) error
	CheckpointSequence(*shardStatus) error
	FetchCheckpoint(*shardStatus) error
}

// ErrSequenceIDNotFound is returned by FetchCheckpoint when no SequenceID is found
var ErrSequenceIDNotFound = errors.New("SequenceIDNotFoundForShard")

// DynamoCheckpoint implements the Checkpoint interface using DynamoDB as a backend
type DynamoCheckpoint struct {
	TableName          string
	LeaseDuration      int
	Retries            int
	ReadCapacityUnits  *int64
	WriteCapacityUnits *int64
	BillingMode        *string
	svc                dynamodbiface.DynamoDBAPI
	skipTableCheck     bool
}

// Init initialises the DynamoDB Checkpoint
func (checkpointer *DynamoCheckpoint) Init() error {
	log.Debug("Creating DynamoDB session")
	session, err := session.NewSessionWithOptions(
		session.Options{
			Config:            aws.Config{Retryer: client.DefaultRetryer{NumMaxRetries: checkpointer.Retries}},
			SharedConfigState: session.SharedConfigEnable,
		},
	)
	if err != nil {
		return err
	}

	if endpoint := os.Getenv("DYNAMODB_ENDPOINT"); endpoint != "" {
		log.Infof("Using dynamodb endpoint from environment %s", endpoint)
		session.Config.Endpoint = &endpoint
	}

	if checkpointer.svc == nil {
		checkpointer.svc = dynamodb.New(session)
	}

	if checkpointer.LeaseDuration == 0 {
		checkpointer.LeaseDuration = defaultLeaseDuration
	}

	if checkpointer.BillingMode == nil {
		checkpointer.BillingMode = aws.String("PAY_PER_REQUEST")
	}

	if !checkpointer.skipTableCheck && !checkpointer.doesTableExist() {
		return checkpointer.createTable()
	}
	return nil
}

// GetLease attempts to gain a lock on the given shard
func (checkpointer *DynamoCheckpoint) GetLease(shard *shardStatus, newAssignTo string) error {
	newLeaseTimeout := time.Now().Add(time.Duration(checkpointer.LeaseDuration) * time.Millisecond).UTC()
	newLeaseTimeoutString := newLeaseTimeout.Format(time.RFC3339Nano)
	currentCheckpoint, err := checkpointer.getItem(shard.ID)
	if err != nil {
		return err
	}

	assignedVar, assignedToOk := currentCheckpoint["AssignedTo"]
	leaseVar, leaseTimeoutOk := currentCheckpoint["LeaseTimeout"]
	var conditionalExpression string
	var expressionAttributeValues map[string]*dynamodb.AttributeValue

	if !leaseTimeoutOk || !assignedToOk {
		conditionalExpression = "attribute_not_exists(AssignedTo)"
		if shard.Checkpoint != "" {
			conditionalExpression = conditionalExpression + " AND SequenceID = :id"
			expressionAttributeValues = map[string]*dynamodb.AttributeValue{
				":id": {
					S: &shard.Checkpoint,
				},
			}
		}
	} else {
		assignedTo := *assignedVar.S
		leaseTimeout := *leaseVar.S

		currentLeaseTimeout, err := time.Parse(time.RFC3339Nano, leaseTimeout)
		if err != nil {
			return err
		}
		if !time.Now().UTC().After(currentLeaseTimeout) && assignedTo != newAssignTo {
			return errors.New(ErrLeaseNotAquired)
		}
		log.Debugf("Attempting to get a lock for shard: %s, leaseTimeout: %s, assignedTo: %s", shard.ID, currentLeaseTimeout, assignedTo)
		conditionalExpression = "ShardID = :id AND AssignedTo = :assigned_to AND LeaseTimeout = :lease_timeout"
		expressionAttributeValues = map[string]*dynamodb.AttributeValue{
			":id": {
				S: &shard.ID,
			},
			":assigned_to": {
				S: &assignedTo,
			},
			":lease_timeout": {
				S: &leaseTimeout,
			},
		}
		if shard.Checkpoint != "" {
			conditionalExpression = conditionalExpression + " AND SequenceID = :sid"
			expressionAttributeValues[":sid"] = &dynamodb.AttributeValue{
				S: &shard.Checkpoint,
			}
		}
	}

	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		"ShardID": {
			S: &shard.ID,
		},
		"AssignedTo": {
			S: &newAssignTo,
		},
		"LeaseTimeout": {
			S: &newLeaseTimeoutString,
		},
		"Closed": {
			BOOL: &shard.Closed,
		},
	}

	if shard.ParentShardID != nil {
		marshalledCheckpoint["ParentShardID"] = &dynamodb.AttributeValue{S: shard.ParentShardID}
	}

	if shard.Checkpoint != "" {
		marshalledCheckpoint["SequenceID"] = &dynamodb.AttributeValue{S: &shard.Checkpoint}
	}

	err = checkpointer.conditionalUpdate(conditionalExpression, expressionAttributeValues, marshalledCheckpoint)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return errors.New(ErrLeaseNotAquired)
			}
		}
		return err
	}

	shard.Lock()
	shard.AssignedTo = newAssignTo
	shard.LeaseTimeout = newLeaseTimeout
	shard.Unlock()

	return nil
}

// CheckpointSequence writes a checkpoint at the designated sequence ID
func (checkpointer *DynamoCheckpoint) CheckpointSequence(shard *shardStatus) error {
	leaseTimeout := shard.LeaseTimeout.UTC().Format(time.RFC3339Nano)
	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		"ShardID": {
			S: &shard.ID,
		},
		"SequenceID": {
			S: &shard.Checkpoint,
		},
		"AssignedTo": {
			S: &shard.AssignedTo,
		},
		"LeaseTimeout": {
			S: &leaseTimeout,
		},
		"Closed": {
			BOOL: &shard.Closed,
		},
	}
	if shard.ParentShardID != nil {
		marshalledCheckpoint["ParentShardID"] = &dynamodb.AttributeValue{S: shard.ParentShardID}
	}
	return checkpointer.saveItem(marshalledCheckpoint)
}

// FetchCheckpoint retrieves the checkpoint for the given shard
func (checkpointer *DynamoCheckpoint) FetchCheckpoint(shard *shardStatus) error {
	checkpoint, err := checkpointer.getItem(shard.ID)
	if err != nil {
		return err
	}

	var sequenceID string
	if s, ok := checkpoint["SequenceID"]; ok {
		// Why do we thrown an error here???
		//return ErrSequenceIDNotFound
		sequenceID = *s.S
	}
	log.Debugf("Retrieved Shard Iterator %s", sequenceID)
	shard.Lock()
	defer shard.Unlock()
	shard.Checkpoint = sequenceID

	if assignedTo, ok := checkpoint["Assignedto"]; ok {
		shard.AssignedTo = *assignedTo.S
	}

	if parent, ok := checkpoint["ParentShardID"]; ok && parent.S != nil {
		shard.ParentShardID = aws.String(*parent.S)
	}
	return nil
}

func (checkpointer *DynamoCheckpoint) createTable() error {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("ShardID"),
				AttributeType: aws.String("S"),
			},
		},
		BillingMode: checkpointer.BillingMode,
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("ShardID"),
				KeyType:       aws.String("HASH"),
			},
		},
		TableName: aws.String(checkpointer.TableName),
	}
	if *checkpointer.BillingMode == "PROVISIONED" {
		input.ProvisionedThroughput = &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  checkpointer.ReadCapacityUnits,
			WriteCapacityUnits: checkpointer.WriteCapacityUnits,
		}
	}
	_, err := checkpointer.svc.CreateTable(input)
	return err
}

func (checkpointer *DynamoCheckpoint) doesTableExist() bool {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(checkpointer.TableName),
	}
	_, err := checkpointer.svc.DescribeTable(input)
	return (err == nil)
}

func (checkpointer *DynamoCheckpoint) saveItem(item map[string]*dynamodb.AttributeValue) error {
	return checkpointer.putItem(&dynamodb.PutItemInput{
		TableName: aws.String(checkpointer.TableName),
		Item:      item,
	})
}

func (checkpointer *DynamoCheckpoint) conditionalUpdate(conditionExpression string, expressionAttributeValues map[string]*dynamodb.AttributeValue, item map[string]*dynamodb.AttributeValue) error {
	return checkpointer.putItem(&dynamodb.PutItemInput{
		ConditionExpression:       aws.String(conditionExpression),
		TableName:                 aws.String(checkpointer.TableName),
		Item:                      item,
		ExpressionAttributeValues: expressionAttributeValues,
	})
}

func (checkpointer *DynamoCheckpoint) putItem(input *dynamodb.PutItemInput) error {
	_, err := checkpointer.svc.PutItem(input)
	return err
}

func (checkpointer *DynamoCheckpoint) getItem(shardID string) (map[string]*dynamodb.AttributeValue, error) {
	item, err := checkpointer.svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(checkpointer.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			"ShardID": {
				S: aws.String(shardID),
			},
		},
	})
	return item.Item, err
}
