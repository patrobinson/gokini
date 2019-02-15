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
	@docker-compose up -d
	@sleep 10
	@go test -timeout 60s -tags=integration
	@docker-compose down
