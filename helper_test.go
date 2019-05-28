package gokini

import (
	"os"
	"time"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	awsclient "github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/kinesis"

	log "github.com/sirupsen/logrus"
)

func createStream(streamName string, shards int64) error {
	session, err := session.NewSessionWithOptions(
		session.Options{
			SharedConfigState: session.SharedConfigEnable,
			Config: aws.Config{
				CredentialsChainVerboseErrors: aws.Bool(true),
				Endpoint:                      aws.String(os.Getenv("KINESIS_ENDPOINT")),
				Retryer:                       awsclient.DefaultRetryer{NumMaxRetries: 1},
			},
		},
	)
	if err != nil {
		return fmt.Errorf("Error starting kinesis client %s", err)
	}
	svc := kinesis.New(session)
	_, err = svc.CreateStream(&kinesis.CreateStreamInput{
		ShardCount: aws.Int64(shards),
		StreamName: aws.String(streamName),
	})
	if err != nil {
		return err
	}
	time.Sleep(500 * time.Millisecond)
	return nil
}

func pushRecordToKinesis(streamName string, record []byte, create bool) error {
	session, err := session.NewSessionWithOptions(
		session.Options{
			SharedConfigState: session.SharedConfigEnable,
			Config: aws.Config{
				CredentialsChainVerboseErrors: aws.Bool(true),
				Endpoint:                      aws.String(os.Getenv("KINESIS_ENDPOINT")),
				Retryer:                       awsclient.DefaultRetryer{NumMaxRetries: 1},
			},
		},
	)
	if err != nil {
		log.Errorf("Error starting kinesis client %s", err)
		return err
	}
	svc := kinesis.New(session)
	if create {
		if err := createStream(streamName, 1); err != nil {
			return err
		}
	}
	_, err = svc.PutRecord(&kinesis.PutRecordInput{
		Data:         record,
		PartitionKey: aws.String("abc123"),
		StreamName:   &streamName,
	})
	if err != nil {
		log.Errorf("Error sending data to kinesis %s", err)
	}
	return err
}

func deleteStream(streamName string) {
	session, _ := session.NewSessionWithOptions(
		session.Options{
			SharedConfigState: session.SharedConfigEnable,
			Config: aws.Config{
				CredentialsChainVerboseErrors: aws.Bool(true),
				Endpoint:                      aws.String(os.Getenv("KINESIS_ENDPOINT")),
			},
		},
	)
	svc := kinesis.New(session)
	_, err := svc.DeleteStream(&kinesis.DeleteStreamInput{
		StreamName: &streamName,
	})
	if err != nil {
		log.Errorln(err)
	}
}

func deleteTable(tableName string) {
	session, _ := session.NewSessionWithOptions(
		session.Options{
			Config: aws.Config{
				Endpoint: aws.String(os.Getenv("DYNAMODB_ENDPOINT")),
			},
			SharedConfigState: session.SharedConfigEnable,
		},
	)
	svc := dynamodb.New(session)
	_, err := svc.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: &tableName,
	})
	if err != nil {
		log.Errorln(err)
	}
}
