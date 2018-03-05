package gokini

import (
	"errors"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/matryer/try"
	log "github.com/sirupsen/logrus"
)

// Checkpointer handles checkpointing when a record has been processed
type Checkpointer interface {
	Init() error
	CheckpointSequence(string, *string, string) error
	FetchCheckpoint(string) (*string, error)
}

// ErrSequenceIDNotFound is returned by FetchCheckpoint when no SequenceID is found
var ErrSequenceIDNotFound = errors.New("SequenceIDNotFoundForShard")

// DynamoCheckpoint implements the Checkpoint interface using DynamoDB as a backend
type DynamoCheckpoint struct {
	TableName string
	svc       dynamodbiface.DynamoDBAPI
	Retries   int
}

// Init initialises the DynamoDB Checkpoint
func (checkpointer *DynamoCheckpoint) Init() error {
	log.Debug("Creating DynamoDB session")
	session, err := session.NewSessionWithOptions(
		session.Options{
			SharedConfigState: session.SharedConfigEnable,
		},
	)
	if err != nil {
		return err
	}

	if endpoint := os.Getenv("DYNAMODB_ENDPOINT"); endpoint != "" {
		session.Config.Endpoint = aws.String(endpoint)
	}

	checkpointer.svc = dynamodb.New(session)

	if !checkpointer.doesTableExist() {
		return checkpointer.createTable()
	}
	return nil
}

// CheckpointSequence writes a checkpoint at the designated sequence ID
func (checkpointer *DynamoCheckpoint) CheckpointSequence(shardID string, sequenceID *string, assignedTo string) error {
	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		"ShardID": {
			S: &shardID,
		},
		"SequenceID": {
			S: sequenceID,
		},
		"AssignedTo": {
			S: &assignedTo,
		},
	}
	return checkpointer.saveItem(marshalledCheckpoint)
}

// FetchCheckpoint retrieves the checkpoint for the given shard
func (checkpointer *DynamoCheckpoint) FetchCheckpoint(shardID string) (*string, error) {
	checkpoint, err := checkpointer.getItem(shardID)
	if err != nil {
		return nil, err
	}

	sequenceID, ok := checkpoint["SequenceID"]
	if !ok || sequenceID.S == nil {
		return nil, ErrSequenceIDNotFound
	}
	log.Debugf("Retrieved Shard Iterator %s", *sequenceID.S)
	return (*sequenceID).S, nil
}

func (checkpointer *DynamoCheckpoint) createTable() error {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("ShardID"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("ShardID"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		TableName: aws.String(checkpointer.TableName),
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
	return try.Do(func(attempt int) (bool, error) {
		_, err := checkpointer.svc.PutItem(&dynamodb.PutItemInput{
			TableName: aws.String(checkpointer.TableName),
			Item:      item,
		})
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeProvisionedThroughputExceededException ||
				awsErr.Code() == dynamodb.ErrCodeInternalServerError &&
					attempt < checkpointer.Retries {
				// Backoff time as recommended by https://docs.aws.amazon.com/general/latest/gr/api-retries.html
				time.Sleep(time.Duration(2^attempt*100) * time.Millisecond)
				return true, err
			}
		}
		return false, err
	})
}

func (checkpointer *DynamoCheckpoint) getItem(shardID string) (map[string]*dynamodb.AttributeValue, error) {
	var item *dynamodb.GetItemOutput
	err := try.Do(func(attempt int) (bool, error) {
		var err error
		item, err = checkpointer.svc.GetItem(&dynamodb.GetItemInput{
			TableName: aws.String(checkpointer.TableName),
			Key: map[string]*dynamodb.AttributeValue{
				"ShardID": {
					S: aws.String(shardID),
				},
			},
		})
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeProvisionedThroughputExceededException ||
				awsErr.Code() == dynamodb.ErrCodeInternalServerError &&
					attempt < checkpointer.Retries {
				// Backoff time as recommended by https://docs.aws.amazon.com/general/latest/gr/api-retries.html
				time.Sleep(time.Duration(2^attempt*100) * time.Millisecond)
				return true, err
			}
		}
		return false, err
	})
	return item.Item, err
}
