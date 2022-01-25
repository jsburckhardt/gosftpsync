lint:
	golangci-lint run --timeout=20m

vet:
	go vet

fmt:
	gofmt -l -w -s .

.PHONY: build
build:
	env GOOS=windows GOARCH=amd64 go build gosftpsync.go
	env GOOS=linux GOARCH=amd64 go build gosftpsync.go

test-ci:
	go-acc ./... -- -v -coverprofile=cover.out 2>&1 | go-junit-report > report.xml
	gocov convert cover.out | gocov-xml > coverage.xml

prepare-checkin: lint
	go mod tidy
