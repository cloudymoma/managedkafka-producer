init:
	go mod init main
	go get github.com/confluentinc/confluent-kafka-go/v2
	go mod tidy

update:
	go get -u github.com/confluentinc/confluent-kafka-go/v2/kafka

build: main.go
	go build -o main main.go

run: build 
	./main

clean: 
	@-rm -rf main 

.PHONY: init run clean
