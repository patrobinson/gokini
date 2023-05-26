package gokini

import (
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"testing"
	"testing/quick"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/google/uuid"
	"github.com/mixer/clock"
)

type DummyRecordConsumer struct {
	shardID string
}

func (p *DummyRecordConsumer) Init(shardID string) error {
	p.shardID = shardID
	return nil
}

func (p *DummyRecordConsumer) ProcessRecords(records []*Records, consumer *KinesisConsumer) {
}

func (p *DummyRecordConsumer) Shutdown() {}

func randMethods(r *rand.Rand) (values []string) {
	events := []string{
		"GetLease",
		"RenewLease",
		"ClaimShard",
		"CheckpointSequence",
	}
	numMethods := r.Intn(20) + 1
	for i := 0; i <= numMethods; i++ {
		values = append(values, events[r.Intn(4)])
	}
	return
}

func doesAllocateShards(in []string, name string) bool {
	streamName := name
	tableName := name
	shards, err := createStream(streamName, 1)
	defer deleteStream(streamName)
	defer deleteTable(tableName)
	if err != nil || len(shards) < 1 {
		panic(err)
	}
	checkpointer := &DynamoCheckpoint{
		TableName:     tableName,
		Session:       session.New(),
		LeaseDuration: 200,
	}
	checkpointer.Init()
	m, _ := NewShardStateMachine(shards[0], nil, checkpointer)
	date := time.Now()
	c := clock.NewMockClock(date)
	m.Clock = c
	consumerID := uuid.New().String()
	for _, method := range in {
		meth := reflect.ValueOf(m).MethodByName(method)
		args := []reflect.Value{reflect.ValueOf(consumerID)}
		if method == "CheckpointSequence" {
			args = []reflect.Value{reflect.ValueOf("0"), reflect.ValueOf(consumerID)}
		}
		meth.Call(args)
	}
	rc := &DummyRecordConsumer{}
	kc := &KinesisConsumer{
		StreamName:               streamName,
		ShardIteratorType:        "TRIM_HORIZON",
		TableName:                tableName,
		EmptyRecordBackoffMs:     50,
		LeaseDuration:            200,
		eventLoopSleepMs:         100,
		millisecondsBackoffClaim: 200,
		RecordConsumer:           rc,
	}
	kc.StartConsumer()
	c.AddTime((time.Duration(kc.LeaseDuration) + 5000) * time.Millisecond)
	time.Sleep(500 * time.Millisecond)
	if s, ok := kc.shards[*shards[0]]; ok {
		if s.AssignedTo() == kc.consumerID {
			return true
		}
	}
	fmt.Println(kc.consumerID)
	fmt.Printf("%+v\n", kc.shards[*shards[0]].shardStatus)
	fmt.Println(c.Now().UTC())
	fmt.Println(kc.shards[*shards[0]].shardStatus.leaseTimeout)
	return false
}

func TestShardAllocation(t *testing.T) {
	//log.SetLevel(log.TraceLevel)
	c := quick.Config{MaxCount: 10000,
		Values: func(values []reflect.Value, r *rand.Rand) {
			values[0] = reflect.ValueOf(randMethods(r))
			values[1] = reflect.ValueOf(fmt.Sprintf("doesAllocateShards%d", r.Uint64()))
		}}
	if err := quick.Check(doesAllocateShards, &c); err != nil {
		t.Error(err)
	}
}
