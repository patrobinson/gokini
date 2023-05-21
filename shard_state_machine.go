package gokini

import (
	"errors"
	"fmt"
	Sync "sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	log "github.com/sirupsen/logrus"
)

const (
	ErrClaimNotAcquired = "Failed to claim shard"
	ErrShardClosed      = "Shard is closed"
	// ErrLeaseNotAquired is returned when we failed to get a lock on the shard
	ErrLeaseNotAcquired = "Lease is already held by another node"
)

type ShardStateMachine struct {
	Unallocated ShardState
	Allocated   ShardState
	BeingStolen ShardState
	Closed      ShardState

	CurrentState ShardState

	*shardStatus
	dc *DynamoCheckpoint
}

func NewShardStateMachine(ShardID, ParentShardID *string, dc *DynamoCheckpoint) (*ShardStateMachine, error) {
	m := ShardStateMachine{
		shardStatus: &shardStatus{
			id:            *ShardID,
			parentShardID: ParentShardID,
		},
		dc: dc,
	}
	unallocatedState := &UnallocatedShard{
		machine: m,
	}
	allocatedState := &AllocatedShard{
		machine: m,
	}
	beingStolenState := &BeingStolenShard{
		machine: m,
	}
	closedState := &ClosedShard{
		machine: m,
	}

	m.Unallocated = unallocatedState
	m.Allocated = allocatedState
	m.BeingStolen = beingStolenState
	m.Closed = closedState

	if err := m.Sync(dc); err != nil {
		return nil, err
	} else if m.AssignedTo() == "" || m.LeaseTimeout().After(time.Now()) {
		m.SetState((m.Unallocated))
	} else if m.ClaimRequest() != nil {
		m.SetState(m.BeingStolen)
	} else {
		m.SetState(m.Allocated)
	}
	return &m, nil
}

func (s *ShardStateMachine) GetLease(consumerID string) error {
	return s.CurrentState.GetLease(consumerID)
}

func (s *ShardStateMachine) RenewLease(consumerID string) error {
	return s.CurrentState.RenewLease(consumerID)
}

func (s *ShardStateMachine) ClaimShard(consumerID string) error {
	return s.CurrentState.ClaimShard(consumerID)
}

func (s *ShardStateMachine) CheckpointSequence(checkpoint, consumerID string) error {
	return s.CurrentState.CheckpointSequence(checkpoint, consumerID)
}

func (s *ShardStateMachine) Close(checkpoint, consumerID string) error {
	return s.CurrentState.Close(checkpoint, consumerID)
}

func (s *ShardStateMachine) SetState(state ShardState) {
	s.CurrentState = state
}

/*
#############
*/

type ShardState interface {
	GetLease(string) error
	RenewLease(string) error
	ClaimShard(string) error
	CheckpointSequence(string, string) error
	Close(string, string) error
}

type UnallocatedShard struct {
	machine ShardStateMachine
}

func (s *UnallocatedShard) GetLease(consumerID string) error {
	err := s.machine.Sync(s.machine.dc)
	if err != nil {
		return err
	}

	if s.machine.AssignedTo() != "" {
		return errors.New(ErrLeaseNotAcquired)
	}
	newLeaseTimeout := time.Now().Add(time.Duration(s.machine.dc.LeaseDuration) * time.Millisecond).UTC()
	newLeaseTimeoutString := newLeaseTimeout.Format(time.RFC3339Nano)
	cond := expression.Name("AssignedTo").AttributeNotExists()
	err = s.machine.TakeLease(s.machine.dc, newLeaseTimeoutString, consumerID, cond)
	if err == nil {
		s.machine.SetState(s.machine.Allocated)
	}
	return err
}

func (s *UnallocatedShard) RenewLease(_ string) error {
	return fmt.Errorf("No lease exists")
}

func (s *UnallocatedShard) CheckpointSequence(_, _ string) error {
	return fmt.Errorf("No lease exists")
}

func (s *UnallocatedShard) ClaimShard(_ string) error {
	return fmt.Errorf("No lease exists to claim")
}

func (s *UnallocatedShard) Close(_, _ string) error {
	return fmt.Errorf("No lease exists")
}

type AllocatedShard struct {
	machine ShardStateMachine
}

func (s *AllocatedShard) GetLease(consumerID string) error {
	err := s.machine.Sync(s.machine.dc)
	if err != nil {
		return err
	}

	if s.machine.LeaseTimeout().After(time.Now()) {
		return errors.New(ErrLeaseNotAcquired)
	}

	newLeaseTimeout := time.Now().Add(time.Duration(s.machine.dc.LeaseDuration) * time.Millisecond).UTC()
	newLeaseTimeoutString := newLeaseTimeout.Format(time.RFC3339Nano)
	cond := expression.Name("AssignedTo").Equal(expression.Value(s.machine.AssignedTo()))
	cond = cond.And(
		expression.Name("LeaseTimeout").Equal(expression.Value(s.machine.LeaseTimeout())),
		expression.Name("SequenceID").Equal(expression.Value(s.machine.Checkpoint())),
	)
	return s.machine.TakeLease(s.machine.dc, newLeaseTimeoutString, consumerID, cond)
}

func (s *AllocatedShard) RenewLease(consumerID string) error {
	err := s.machine.Sync(s.machine.dc)
	if err != nil {
		return err
	}
	if s.machine.AssignedTo() != consumerID || s.machine.relinquishShard == true {
		return errors.New(ErrLeaseNotAcquired)
	}

	if cr := s.machine.ClaimRequest(); cr != nil || *cr != "" {
		return errors.New(ErrLeaseNotAcquired)
	}
	newLeaseTimeout := time.Now().Add(time.Duration(s.machine.dc.LeaseDuration) * time.Millisecond).UTC()
	newLeaseTimeoutString := newLeaseTimeout.Format(time.RFC3339Nano)
	cond := expression.Name("AssignedTo").Equal(expression.Value(consumerID))
	cond = cond.And(
		expression.Name("ClaimRequest").AttributeNotExists(),
	)
	return s.machine.TakeLease(s.machine.dc, newLeaseTimeoutString, consumerID, cond)
}

func (s *AllocatedShard) CheckpointSequence(checkpoint string, consumerID string) error {
	var relinquish bool
	if s.machine.ClaimRequest() != nil {
		s.machine.SetState(s.machine.BeingStolen)
		relinquish = true
	}

	return s.machine.UpdateCheckpoint(s.machine.dc, consumerID, checkpoint, false, relinquish)
}

func (s *AllocatedShard) ClaimShard(consumerID string) error {
	err := s.machine.Sync(s.machine.dc)
	if err != nil {
		return err
	}
	if s.machine.AssignedTo() == "" {
		return errors.New(ErrClaimNotAcquired)
	}
	err = s.machine.TakeClaim(s.machine.dc, consumerID)
	if err == nil {
		s.machine.SetState(s.machine.BeingStolen)
	}
	return err
}

func (s *AllocatedShard) Close(checkpoint string, consumerID string) error {
	err := s.machine.UpdateCheckpoint(s.machine.dc, consumerID, checkpoint, true, false)
	if err == nil {
		s.machine.SetState(s.machine.Closed)
	}
	return err
}

type BeingStolenShard struct {
	machine ShardStateMachine
}

func (s *BeingStolenShard) GetLease(consumerID string) error {
	err := s.machine.Sync(s.machine.dc)
	if err != nil {
		return err
	}

	if cr := s.machine.ClaimRequest(); cr == nil || *cr != consumerID {
		return errors.New(ErrLeaseNotAcquired)
	}

	newLeaseTimeout := time.Now().Add(time.Duration(s.machine.dc.LeaseDuration) * time.Millisecond).UTC()
	newLeaseTimeoutString := newLeaseTimeout.Format(time.RFC3339Nano)
	cond := expression.Name("AssignedTo").Equal(expression.Value(s.machine.ID()))
	cond = cond.And(
		expression.Name("ClaimRequest").Equal(expression.Value(consumerID)),
		expression.Name("RelinquishShard").Equal(expression.Value(true)),
	)
	err = s.machine.TakeLease(s.machine.dc, newLeaseTimeoutString, consumerID, cond)
	if err == nil {
		s.machine.SetState(s.machine.Allocated)
	}
	return err
}

func (s *BeingStolenShard) RenewLease(_ string) error {
	return fmt.Errorf("Shard is being stolen")
}

func (s *BeingStolenShard) ClaimShard(_ string) error {
	// Should be ok even if the consumer stealing the shard disappears.
	// Because the timeout will be reached and it will be allocated again
	return fmt.Errorf("Shard is already being stolen")
}

func (s *BeingStolenShard) CheckpointSequence(checkpoint string, consumerID string) error {
	return s.machine.UpdateCheckpoint(s.machine.dc, consumerID, checkpoint, false, false)
}

func (s *BeingStolenShard) Close(checkpoint string, consumerID string) error {
	err := s.machine.UpdateCheckpoint(s.machine.dc, consumerID, checkpoint, true, false)
	if err == nil {
		s.machine.SetState(s.machine.Closed)
	}
	return err
}

type ClosedShard struct {
	machine ShardStateMachine
}

func (s *ClosedShard) GetLease(_ string) error {
	return errors.New(ErrShardClosed)
}

func (s *ClosedShard) RenewLease(_ string) error {
	return errors.New(ErrShardClosed)
}

func (s *ClosedShard) CheckpointSequence(_, _ string) error {
	return errors.New(ErrShardClosed)
}

func (s *ClosedShard) ClaimShard(_ string) error {
	return errors.New(ErrShardClosed)
}

func (s *ClosedShard) Close(_, _ string) error {
	return errors.New(ErrShardClosed)
}

/*
#############
*/

// This should be private. Might want to move this file into it's own module to enforece that
type shardStatus struct {
	id              string
	checkpoint      string
	assignedTo      string
	leaseTimeout    time.Time
	parentShardID   *string
	closed          bool
	claimRequest    *string
	relinquishShard bool
	readyToBeClosed bool
	Sync.Mutex
}

func (shard *shardStatus) ID() string {
	return shard.id
}

func (shard *shardStatus) AssignedTo() string {
	return shard.assignedTo
}

func (shard *shardStatus) Checkpoint() string {
	return shard.checkpoint
}

func (shard *shardStatus) ParentShardID() *string {
	return shard.parentShardID
}

func (shard *shardStatus) ClaimRequest() *string {
	return shard.claimRequest
}

func (shard *shardStatus) LeaseTimeout() time.Time {
	return shard.leaseTimeout
}

func (shard *shardStatus) TakeClaim(c *DynamoCheckpoint, consumerID string) error {
	if shard.ID() == consumerID {
		return fmt.Errorf("shard already belongs to this consumer")
	}
	cond := expression.ConditionBuilder(
		expression.Name("SequenceID").Equal(expression.Value(shard.Checkpoint())),
	)
	cond = cond.And(
		expression.Name("ClaimRequest").AttributeNotExists(),
	)
	update := expression.Set(
		expression.Name("ClaimRequest"),
		expression.Value(consumerID),
	)

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
				S: aws.String(shard.ID()),
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
				return errors.New(ErrClaimNotAcquired)
			}
		}
		return err
	}

	return nil
}

func (shard *shardStatus) TakeLease(c *DynamoCheckpoint, leaseTimeout, consumerID string, cond expression.ConditionBuilder) error {
	if shard.Checkpoint() != "" {
		cond = cond.And(
			expression.Name("SequenceID").Equal(expression.Value(shard.Checkpoint())),
		)
	}

	update := expression.Set(
		expression.Name("AssignedTo"),
		expression.Value(consumerID),
	).Set(
		expression.Name("LeaseTimeout"),
		expression.Value(leaseTimeout),
	).Set(
		expression.Name("RelinquishShard"),
		expression.Value(false),
	)
	if shard.ParentShardID() != nil {
		update.Set(
			expression.Name("ParentShardID"),
			expression.Value(shard.ParentShardID()),
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
				S: aws.String(shard.ID()),
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
				return errors.New(ErrLeaseNotAcquired)
			}
		}
		return err
	}

	return nil
}

func (shard *shardStatus) UpdateCheckpoint(c *DynamoCheckpoint, consumerID, checkpoint string, closed, relinquish bool) error {
	update := expression.Set(
		expression.Name("SequenceID"),
		expression.Value(checkpoint),
	)

	update = update.Set(
		expression.Name("RelinquishShard"),
		expression.Value(relinquish),
	)

	update = update.Set(
		expression.Name("Closed"),
		expression.Value(closed),
	)

	cond := expression.Name("AssignedTo").Equal(expression.Value(consumerID))
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
				S: aws.String(shard.ID()),
			},
		},
		UpdateExpression: expr.Update(),
	})
	return err
}

func (shard *shardStatus) Sync(c *DynamoCheckpoint) error {
	checkpoint, err := c.getItem(shard.id)
	if err != nil {
		return err
	}

	var sequenceID string
	if s, ok := checkpoint["SequenceID"]; ok {
		sequenceID = *s.S
	}
	log.Debugf("Retrieved Shard Iterator %s", sequenceID)
	shard.Lock()
	defer shard.Unlock()
	shard.checkpoint = sequenceID

	if assignedTo, ok := checkpoint["AssignedTo"]; ok {
		shard.assignedTo = *assignedTo.S
	}

	if parent, ok := checkpoint["ParentShardID"]; ok && parent.S != nil {
		shard.parentShardID = parent.S
	}

	if claim, ok := checkpoint["ClaimRequest"]; ok && claim.S != nil {
		shard.claimRequest = claim.S
	}

	if lease, ok := checkpoint["LeaseTimeout"]; ok && lease.S != nil {
		currentLeaseTimeout, err := time.Parse(time.RFC3339Nano, *lease.S)
		if err != nil {
			return err
		}
		shard.leaseTimeout = currentLeaseTimeout
	}

	if relinquishShard, ok := checkpoint["RelinquishShard"]; ok && relinquishShard.BOOL != nil {
		shard.relinquishShard = *relinquishShard.BOOL
	}
	log.Debugln("Shard updated", *shard)
	return nil
}
