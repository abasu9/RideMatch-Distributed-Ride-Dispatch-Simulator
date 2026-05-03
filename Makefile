.PHONY: proto-sync tidy test docker-build

SERVICE ?= driver

proto-sync:
	cp proto/driver.proto internal/proto/schemas/driver.proto
	cp proto/matching.proto internal/proto/schemas/matching.proto
	cp proto/rider.proto internal/proto/schemas/rider.proto

tidy:
	go mod tidy

test:
	go test ./...

docker-build:
	test -n "$(SERVICE)"
	docker compose build "$(SERVICE)"
