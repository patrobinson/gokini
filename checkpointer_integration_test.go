//+build integration

package gokini

import (
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestRaceCondGetLeaseTimeout(t *testing.T) {
	checkpoint := &DynamoCheckpoint{
		TableName: "TableName",
	}
	checkpoint.Init()
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
	checkpoint := &DynamoCheckpoint{
		TableName: "TableName",
	}
	checkpoint.Init()
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
