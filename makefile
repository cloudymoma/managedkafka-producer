init:
	go mod init main
	go get github.com/confluentinc/confluent-kafka-go/kafka
	go mod tidy

build: main.go
	go build -o main main.go

run: build 
	./main

clean: 
	@-rm -rf main 

.PHONY: init run clean
