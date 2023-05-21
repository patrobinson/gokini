package gokini

import (
	"errors"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	log "github.com/sirupsen/logrus"
)

// TODO: We have to deal with possibly empty attributes in a lot of places in here
// This is error prone and likely to create race conditions
// Create a method that will update all attributes, in a conditional update, based on all attributes in shard

const (
	defaultLeaseDuration = 30000
	// ErrInvalidDynamoDBSchema is returned when there are one or more fields missing from the table
	ErrInvalidDynamoDBSchema = "The DynamoDB schema is invalid and may need to be re-created"
)

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
