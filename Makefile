TIMEOUT  = 30

get:
	@go get ./...

check test tests:
	@go test -short -timeout $(TIMEOUT)s ./...

integration: get
	@go test -timeout 30s -tags=integration
	@go test -run TestRebalance -count 25 -tags=integration

docker-integration:
	@docker-compose run gokini make integration
	@docker-compose down
