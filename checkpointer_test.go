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
	log "github.com/sirupsen/logrus"
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
	log.SetLevel(log.DebugLevel)
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
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String("TableName"),
		Item:      marshalledCheckpoint,
	}
	checkpoint.svc.PutItem(input)
	err := checkpoint.GetLease(&shardStatus{
		ID:         "0001",
		Checkpoint: "",
		mux:        &sync.Mutex{},
	}, "ijkl-mnop")

	if err != nil {
		t.Errorf("Lease not aquired after timeout %s", err)
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
