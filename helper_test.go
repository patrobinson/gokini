package gokini

import (
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	log "github.com/sirupsen/logrus"
)

func pushRecordToKinesis(streamName string, record []byte) error {
	session, err := session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable})
	if err != nil {
		log.Errorf("Error starting kinesis client %s", err)
		return err
	}
	session.Config.Endpoint = aws.String(os.Getenv("KINESIS_ENDPOINT"))
	svc := kinesis.New(session)
	_, err = svc.CreateStream(&kinesis.CreateStreamInput{
		ShardCount: aws.Int64(1),
		StreamName: aws.String(streamName),
	})
	if err != nil {
		log.Errorf("Error creating kinesis stream %s", err)
	}
	time.Sleep(500 * time.Millisecond)
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
