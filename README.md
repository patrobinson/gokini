# Gokini

[![GoDoc](https://godoc.org/github.com/golang/gddo?status.svg)](https://godoc.org/github.com/patrobinson/gokini)
[![Build
Status](https://travis-ci.org/golang/gddo.svg?branch=master)](https://travis-ci.org/patrobinson/gokini)

A Golang Kinesis Consumer Library with minimal dependencies. This library does not depend on the Java MultiLangDaemon but does use the AWS SDK.

## Project Goals

This project aims to provide feature parity with the [Kinesis Client Library](https://github.com/awslabs/amazon-kinesis-client) including:

- [x] Enumerates shards

- [x] Coordinates shard associations with other workers

- [x] Instantiates a record processor for every shard it manages

- [x] Checkpoints processed records

- [ ] Balances shard-worker associations when the worker instance count changes

- [ ] Balances shard-worker associations when shards are split or merged

- [x] Instrumentation that supports CloudWatch (partial support)

## Development Status

Beta - Ready to be used in non-critical Production environments.

## Testing

Unit tests can be run with:
```
go test consumer_test.go consumer.go checkpointer_test.go checkpointer.go
```

Integration tests can be run in docker with:
```
make docker-integration
```
