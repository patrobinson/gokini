//+build integration

package gokini

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestGetLeaseNotAquired(t *testing.T) {
	checkpoint := &DynamoCheckpoint{
		TableName: "TableName",
		Session:   session.New(),
	}
	checkpoint.Init()
	defer checkpoint.svc.DeleteTable(&dynamodb.DeleteTableInput{TableName: aws.String("TableName")})
	err := checkpoint.GetLease(&shardStatus{
		ID:         "0001",
		Checkpoint: "",
	}, "abcd-efgh")
	if err != nil {
		t.Errorf("Error getting lease %s", err)
	}

	err = checkpoint.GetLease(&shardStatus{
		ID:         "0001",
		Checkpoint: "",
	}, "ijkl-mnop")
	if err == nil || err.Error() != ErrLeaseNotAquired {
		t.Errorf("Got a lease when it was already held by abcd-efgh: %s", err)
	}
}

func TestGetLeaseAquired(t *testing.T) {
	checkpoint := &DynamoCheckpoint{
		TableName: "TableName",
		Session:   session.New(),
	}
	checkpoint.Init()
	defer checkpoint.svc.DeleteTable(&dynamodb.DeleteTableInput{TableName: aws.String("TableName")})
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
	}
	err := checkpoint.GetLease(shard, "ijkl-mnop")

	if err != nil {
		t.Errorf("Lease not aquired after timeout %s", err)
		t.Log(checkpoint.svc.GetItem(&dynamodb.GetItemInput{TableName: aws.String("TableName"),
			Key: map[string]*dynamodb.AttributeValue{
				"ShardID": {
					S: aws.String("0001"),
				},
			}}))
	}

	checkpoint.FetchCheckpoint(shard)

	if shard.Checkpoint != "deadbeef" {
		t.Errorf("Expected SequenceID to be deadbeef. Got '%s'", shard.Checkpoint)
	}
}

func TestRaceCondGetLeaseTimeout(t *testing.T) {
	checkpoint := &DynamoCheckpoint{
		TableName: "TableName",
		Session:   session.New(),
	}
	checkpoint.Init()
	defer checkpoint.svc.DeleteTable(&dynamodb.DeleteTableInput{TableName: aws.String("TableName")})
	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		"ShardID": {
			S: aws.String("0001"),
		},
		"AssignedTo": {
			S: aws.String("abcd-efgh"),
		},
		"LeaseTimeout": {
			S: aws.String(time.Now().AddDate(0, 0, 1).UTC().Format(time.RFC3339)),
		},
		"SequenceID": {
			S: aws.String("deadbeef"),
		},
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String("TableName"),
		Item:      marshalledCheckpoint,
	}
	_, err := checkpoint.svc.PutItem(input)
	if err != nil {
		t.Fatalf("Error writing to dynamo %s", err)
	}
	shard := &shardStatus{
		ID:         "0001",
		Checkpoint: "TestRaceCondGetLeaseTimeout",
	}
	err = checkpoint.GetLease(shard, "ijkl-mnop")

	if err == nil || err.Error() != ErrLeaseNotAquired {
		t.Error("Got a lease when shard was assigned.")
	}
}
func TestRaceCondGetLeaseNoAssignee(t *testing.T) {
	checkpoint := &DynamoCheckpoint{
		TableName: "TableName",
		Session:   session.New(),
	}
	checkpoint.Init()
	defer checkpoint.svc.DeleteTable(&dynamodb.DeleteTableInput{TableName: aws.String("TableName")})
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
	_, err := checkpoint.svc.PutItem(input)
	if err != nil {
		t.Fatalf("Error writing to dynamo %s", err)
	}
	shard := &shardStatus{
		ID:         "0001",
		Checkpoint: "TestRaceCondGetLeaseNoAssignee",
	}
	err = checkpoint.GetLease(shard, "ijkl-mnop")

	if err == nil || err.Error() != ErrLeaseNotAquired {
		t.Errorf("Got a lease when checkpoints didn't match. Potentially we stomped on the checkpoint %s", err)
	}
}

func TestGetLeaseRenewed(t *testing.T) {
	checkpoint := &DynamoCheckpoint{
		TableName:     "TableName",
		Session:       session.New(),
		LeaseDuration: 0,
	}
	checkpoint.Init()
	defer checkpoint.svc.DeleteTable(&dynamodb.DeleteTableInput{TableName: aws.String("TableName")})
	err := checkpoint.GetLease(&shardStatus{
		ID:         "0001",
		Checkpoint: "",
	}, "abcd-efgh")
	if err != nil {
		t.Errorf("Error getting lease %s", err)
	}

	err = checkpoint.GetLease(&shardStatus{
		ID:         "0001",
		Checkpoint: "",
	}, "abcd-efgh")
	if err != nil {
		t.Errorf("Error renewing lease %s", err)
	}
}
