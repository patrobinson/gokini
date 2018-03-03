package gokini

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type mockDynamoTableExist struct {
	dynamodbiface.DynamoDBAPI
	tableExist bool
}

func (m *mockDynamoTableExist) DescribeTable(*dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
	if !m.tableExist {
		return &dynamodb.DescribeTableOutput{}, awserr.New(dynamodb.ErrCodeResourceNotFoundException, "doesNotExist", errors.New(""))
	}
	return &dynamodb.DescribeTableOutput{}, nil
}

func TestDoesTableExist(t *testing.T) {
	svc := &mockDynamoTableExist{tableExist: true}
	checkpoint := &DynamoCheckpoint{
		TableName: "TableName",
		svc:       svc,
	}
	if !checkpoint.doesTableExist() {
		t.Error("Table exists but returned false")
	}

	svc = &mockDynamoTableExist{tableExist: false}
	checkpoint.svc = svc
	if checkpoint.doesTableExist() {
		t.Error("Table does not exist but returned true")
	}
}
