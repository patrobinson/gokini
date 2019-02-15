package gokini

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type mockDynamoDB struct {
	dynamodbiface.DynamoDBAPI
	tableExist bool
	item       map[string]*dynamodb.AttributeValue
}

func (m *mockDynamoDB) DescribeTable(*dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
	if !m.tableExist {
		return &dynamodb.DescribeTableOutput{}, awserr.New(dynamodb.ErrCodeResourceNotFoundException, "doesNotExist", errors.New(""))
	}
	return &dynamodb.DescribeTableOutput{}, nil
}

func (m *mockDynamoDB) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	m.item = input.Item
	return nil, nil
}

func (m *mockDynamoDB) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	return &dynamodb.GetItemOutput{
		Item: m.item,
	}, nil
}
func TestDoesTableExist(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true}
	checkpoint := &DynamoCheckpoint{
		TableName: "TableName",
		svc:       svc,
	}
	if !checkpoint.doesTableExist() {
		t.Error("Table exists but returned false")
	}

	svc = &mockDynamoDB{tableExist: false}
	checkpoint.svc = svc
	if checkpoint.doesTableExist() {
		t.Error("Table does not exist but returned true")
	}
}

func TestGetLeaseNotAquired(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true}
	checkpoint := &DynamoCheckpoint{
		TableName: "TableName",
		svc:       svc,
	}
	checkpoint.Init()
	checkpoint.svc = svc
	err := checkpoint.GetLease(&shardStatus{
		ID:         "0001",
		Checkpoint: "",
		mux:        &sync.Mutex{},
	}, "abcd-efgh")
	if err != nil {
		t.Errorf("Error getting lease %s", err)
	}

	err = checkpoint.GetLease(&shardStatus{
		ID:         "0001",
		Checkpoint: "",
		mux:        &sync.Mutex{},
	}, "ijkl-mnop")
	if err == nil || err.Error() != ErrLeaseNotAquired {
		t.Errorf("Got a lease when it was already held by abcd-efgh: %s", err)
	}
}

func TestGetLeaseAquired(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true}
	checkpoint := &DynamoCheckpoint{
		TableName: "TableName",
	}
	checkpoint.Init()
	checkpoint.svc = svc
	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		"ShardID": {
			S: aws.String("0001"),
		},
		"AssignedTo": {
			S: aws.String("abcd-efgh"),
		},
		"LeaseTimeout": {
			S: aws.String(time.Now().AddDate(0, -1, 0).UTC().Format(time.RFC3339)),
		},
		"SequenceID": {
			S: aws.String("deadbeef"),
		},
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String("TableName"),
		Item:      marshalledCheckpoint,
	}
	checkpoint.svc.PutItem(input)
	shard := &shardStatus{
		ID:         "0001",
		Checkpoint: "deadbeef",
		mux:        &sync.Mutex{},
	}
	err := checkpoint.GetLease(shard, "ijkl-mnop")

	if err != nil {
		t.Errorf("Lease not aquired after timeout %s", err)
	}

	id, ok := svc.item["SequenceID"]
	if !ok {
		t.Error("Expected SequenceID to be set by GetLease")
	} else if *id.S != "deadbeef" {
		t.Errorf("Expected SequenceID to be deadbeef. Got '%s'", *id.S)
	}
}

func TestRaceCondGetLeaseTimeout(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true}
	checkpoint := &DynamoCheckpoint{
		TableName: "TableName",
	}
	checkpoint.Init()
	checkpoint.svc = svc
	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		"ShardID": {
			S: aws.String("0001"),
		},
		"AssignedTo": {
			S: aws.String("abcd-efgh"),
		},
		"LeaseTimeout": {
			S: aws.String(time.Now().AddDate(0, -1, 0).UTC().Format(time.RFC3339)),
		},
		"SequenceID": {
			S: aws.String("deadbeef"),
		},
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String("TableName"),
		Item:      marshalledCheckpoint,
	}
	checkpoint.svc.PutItem(input)
	shard := &shardStatus{
		ID:         "0001",
		Checkpoint: "",
		mux:        &sync.Mutex{},
	}
	err := checkpoint.GetLease(shard, "ijkl-mnop")

	if err == nil || err.Error() != ErrLeaseNotAquired {
		t.Error("Got a lease when checkpoints didn't match. Potentially we stomped on the checkpoint")
	}
}

func TestRaceCondGetLeaseNoAssignee(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true}
	checkpoint := &DynamoCheckpoint{
		TableName: "TableName",
	}
	checkpoint.Init()
	checkpoint.svc = svc
	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		"ShardID": {
			S: aws.String("0001"),
		},
		"SequenceID": {
			S: aws.String("deadbeef"),
		},
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String("TableName"),
		Item:      marshalledCheckpoint,
	}
	checkpoint.svc.PutItem(input)
	shard := &shardStatus{
		ID:         "0001",
		Checkpoint: "",
		mux:        &sync.Mutex{},
	}
	err := checkpoint.GetLease(shard, "ijkl-mnop")

	if err == nil || err.Error() != ErrLeaseNotAquired {
		t.Error("Got a lease when checkpoints didn't match. Potentially we stomped on the checkpoint")
	}
}

func TestGetLeaseRenewed(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true}
	checkpoint := &DynamoCheckpoint{
		TableName: "TableName",
		svc:       svc,
	}
	checkpoint.Init()
	checkpoint.svc = svc
	err := checkpoint.GetLease(&shardStatus{
		ID:         "0001",
		Checkpoint: "",
		mux:        &sync.Mutex{},
	}, "abcd-efgh")
	if err != nil {
		t.Errorf("Error getting lease %s", err)
	}

	err = checkpoint.GetLease(&shardStatus{
		ID:         "0001",
		Checkpoint: "",
		mux:        &sync.Mutex{},
	}, "abcd-efgh")
	if err != nil {
		t.Errorf("Error renewing lease %s", err)
	}
}
