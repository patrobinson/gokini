TIMEOUT  = 30

get:
	@go get ./...

check test tests:
	@go test -short -timeout $(TIMEOUT)s ./...
