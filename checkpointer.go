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
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	log "github.com/sirupsen/logrus"
)

// TODO: We have to deal with possibly empty attributes in a lot of places in here
// This is error prone and likely to create race conditions
// Create a method that will update all attributes, in a conditional update, based on all attributes in shard

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
	ListActiveWorkers() (map[string][]string, error)
	ClaimShard(*shardStatus, string) error
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
	Session            *session.Session
	svc                dynamodbiface.DynamoDBAPI
	skipTableCheck     bool
}

// Init initialises the DynamoDB Checkpoint
func (c *DynamoCheckpoint) Init() error {
	if endpoint := os.Getenv("DYNAMODB_ENDPOINT"); endpoint != "" {
		log.Infof("Using dynamodb endpoint from environment %s", endpoint)
		c.Session.Config.Endpoint = &endpoint
	}

	if c.svc == nil {
		c.svc = dynamodb.New(c.Session)
	}

	if c.LeaseDuration == 0 {
		c.LeaseDuration = defaultLeaseDuration
	}

	if c.BillingMode == nil {
		c.BillingMode = aws.String("PAY_PER_REQUEST")
	}

	if !c.skipTableCheck && !c.doesTableExist() {
		return c.createTable()
	}
	return nil
}

// GetLease attempts to gain a lock on the given shard
func (c *DynamoCheckpoint) GetLease(shard *shardStatus, newAssignTo string) error {
	newLeaseTimeout := time.Now().Add(time.Duration(c.LeaseDuration) * time.Millisecond).UTC()
	newLeaseTimeoutString := newLeaseTimeout.Format(time.RFC3339Nano)
	currentCheckpoint, err := c.getItem(shard.ID)
	if err != nil {
		return err
	}

	assignedVar, assignedToOk := currentCheckpoint["AssignedTo"]
	leaseVar, leaseTimeoutOk := currentCheckpoint["LeaseTimeout"]

	var cond expression.ConditionBuilder

	if !leaseTimeoutOk || !assignedToOk {
		cond = expression.Name("AssignedTo").AttributeNotExists()
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
		cond = expression.Name("AssignedTo").Equal(expression.Value(assignedTo))
	}
	if shard.Checkpoint != "" {
		cond = cond.And(
			expression.Name("SequenceID").Equal(expression.Value(shard.Checkpoint)),
		)
	}

	update := expression.Set(
		expression.Name("AssignedTo"),
		expression.Value(newAssignTo),
	).Set(
		expression.Name("LeaseTimeout"),
		expression.Value(newLeaseTimeoutString),
	)
	if shard.ParentShardID != nil {
		update.Set(
			expression.Name("ParentShardID"),
			expression.Value(shard.ParentShardID),
		)
	}

	expr, err := expression.NewBuilder().
		WithUpdate(update).
		WithCondition(cond).
		Build()
	if err != nil {
		return err
	}

	i := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConditionExpression:       expr.Condition(),
		TableName:                 &c.TableName,
		Key: map[string]*dynamodb.AttributeValue{
			"ShardID": {
				S: &shard.ID,
			},
		},
		UpdateExpression: expr.Update(),
	}
	log.Traceln(i)
	_, err = c.svc.UpdateItem(i)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				log.Traceln("Condition failed", err)
				return errors.New(ErrLeaseNotAquired)
			}
		}
		return err
	}

	shard.Lock()
	shard.AssignedTo = newAssignTo
	shard.LeaseTimeout = newLeaseTimeout
	shard.ClaimRequest = nil
	shard.Unlock()

	return nil
}

// CheckpointSequence writes a checkpoint at the designated sequence ID
func (c *DynamoCheckpoint) CheckpointSequence(shard *shardStatus) error {
	update := expression.Set(
		expression.Name("SequenceID"),
		expression.Value(shard.Checkpoint),
	).Set(
		expression.Name("Closed"),
		expression.Value(shard.Closed),
	)
	cond := expression.Name("ClaimRequest").AttributeNotExists().And(
		expression.Name("AssignedTo").Equal(expression.Value(shard.AssignedTo)),
	)
	expr, err := expression.NewBuilder().
		WithUpdate(update).
		WithCondition(cond).
		Build()
	if err != nil {
		return err
	}
	_, err = c.svc.UpdateItem(&dynamodb.UpdateItemInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConditionExpression:       expr.Condition(),
		TableName:                 aws.String(c.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			"ShardID": {
				S: &shard.ID,
			},
		},
		UpdateExpression: expr.Update(),
	})
	return err
}

// FetchCheckpoint retrieves the checkpoint for the given shard
func (c *DynamoCheckpoint) FetchCheckpoint(shard *shardStatus) error {
	checkpoint, err := c.getItem(shard.ID)
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

	if assignedTo, ok := checkpoint["AssignedTo"]; ok {
		shard.AssignedTo = *assignedTo.S
	}

	if parent, ok := checkpoint["ParentShardID"]; ok && parent.S != nil {
		shard.ParentShardID = aws.String(*parent.S)
	}

	if claim, ok := checkpoint["ClaimRequest"]; ok && claim.S != nil {
		shard.ClaimRequest = aws.String(*claim.S)
	}

	if lease, ok := checkpoint["LeaseTimeout"]; ok && lease.S != nil {
		currentLeaseTimeout, err := time.Parse(time.RFC3339Nano, *lease.S)
		if err != nil {
			return err
		}
		shard.LeaseTimeout = currentLeaseTimeout
	}
	log.Debugln("Shard updated", *shard)
	return nil
}

type Worker struct {
	UUID   string
	Shards []string
}

func (c *DynamoCheckpoint) ListActiveWorkers() (map[string][]string, error) {
	items, err := c.svc.Scan(&dynamodb.ScanInput{
		TableName: aws.String(c.TableName),
	})
	if err != nil {
		return nil, err
	}
	workers := make(map[string][]string)
	for _, i := range items.Items {
		// Ignore closed shards, only return active shards
		if closed, ok := i["Closed"]; ok && closed.S != nil {
			if *closed.BOOL {
				continue
			}
		}
		var workUUID, shardID string
		if u, ok := i["AssignedTo"]; !ok || u.S == nil {
			return nil, errors.New("invalid value found in DynamoDB table")
		}
		workUUID = *i["AssignedTo"].S
		if s, ok := i["ShardID"]; !ok || s.S == nil {
			return nil, errors.New("invalid value found in DynamoDB table")
		}
		shardID = *i["ShardID"].S
		if w, ok := workers[workUUID]; ok {
			workers[workUUID] = append(w, shardID)
		} else {
			workers[workUUID] = []string{shardID}
		}
	}
	return workers, nil
}

func (c *DynamoCheckpoint) ClaimShard(shard *shardStatus, claimID string) error {
	err := c.FetchCheckpoint(shard)
	if err != nil {
		return err
	}
	update := expression.Set(
		expression.Name("ClaimRequest"),
		expression.Value(claimID),
	)
	cond := expression.Name("ClaimRequest").AttributeNotExists().And(
		expression.Name("AssignedTo").Equal(expression.Value(shard.AssignedTo)),
	)
	expr, err := expression.NewBuilder().
		WithUpdate(update).
		WithCondition(cond).
		Build()
	if err != nil {
		return err
	}
	_, err = c.svc.UpdateItem(&dynamodb.UpdateItemInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConditionExpression:       expr.Condition(),
		TableName:                 aws.String(c.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			"ShardID": {
				S: &shard.ID,
			},
		},
		UpdateExpression: expr.Update(),
	})
	return err
}

func (c *DynamoCheckpoint) createTable() error {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("ShardID"),
				AttributeType: aws.String("S"),
			},
		},
		BillingMode: c.BillingMode,
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("ShardID"),
				KeyType:       aws.String("HASH"),
			},
		},
		TableName: aws.String(c.TableName),
	}
	if *c.BillingMode == "PROVISIONED" {
		input.ProvisionedThroughput = &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  c.ReadCapacityUnits,
			WriteCapacityUnits: c.WriteCapacityUnits,
		}
	}
	_, err := c.svc.CreateTable(input)
	return err
}

func (c *DynamoCheckpoint) doesTableExist() bool {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(c.TableName),
	}
	_, err := c.svc.DescribeTable(input)
	return (err == nil)
}

func (c *DynamoCheckpoint) getItem(shardID string) (map[string]*dynamodb.AttributeValue, error) {
	item, err := c.svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(c.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			"ShardID": {
				S: aws.String(shardID),
			},
		},
	})
	return item.Item, err
}
