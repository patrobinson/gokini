TIMEOUT  = 30

get:
	@go get ./...

check test tests:
	@go test -short -timeout $(TIMEOUT)s ./...

integration: get
	@go test -timeout 30s -tags=integration

docker-integration:
	 @docker-compose run --rm gokini make integration

travis-integration:
	export KINESIS_ENDPOINT=http://localhost:4567 DYNAMODB_ENDPOINT=http://localhost:8000 AWS_DEFAULT_REGION=ap-nil-1 AWS_ACCESS_KEY=AKAILKAJDFLKADJFL AWS_SECRET_KEY=90uda098fjdsoifjsdaoifjpisjf
	@docker-compose up
	@go test -timeout 30s -tags=integration
