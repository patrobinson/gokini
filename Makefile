TIMEOUT  = 30

get:
	@go get ./...

check test tests:
	@go test -short -timeout $(TIMEOUT)s ./...

integration: get
	@go test -timeout 30s -tags=integration

docker-integration:
	@docker-compose up -d
	@sleep 10
	@docker-compose run gokini make integration
	@docker-compose down
