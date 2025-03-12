## Googel Cloud Managed Kafka Data Producer

### TL;DR
Purpose of this producer is used for Google Cloud Managed Kafka load test. Hence you could use that for sizing and cost estimation

Use this [dataflow consumer](https://github.com/cloudymoma/managedkafka-dataflow) to achieve best performance and retrieve stats information.

### Quickstart

#### Prerequisites, the Google Auth Server 

1. install the server

```shell
./authserver install
```

2. start the server

```shell
./authserver start
```

3. check server status

```shell
./authserver status
```

you could check `authserver.log` for running information and errors, or pass
`stop`, `restart` parameters accordingly for server controls.

#### Go Program build

##### code you need to change before build 

- `topicName` is the Kafka Topic you want to send to
- `"bootstrap.servers": "bootstrap.dingo-kafka.us-central1.managedkafka.du-hast-mich.cloud.goog:9092",` replace the value accordingly to your Kafka server
- `case <-time.After(100 * time.Millisecond):`, 1000 means 1 message per second per publisher, smaller number means higher producing rate. Completely turn this off could result 429 push back from Kafka. The program has the backoff strategy, so it shouldn't crash. Use it wisely.
- *Optional*
  - `numPublishers` number of Kafka publishers concurrently
  - `numDataGenThreads` number of data generation threads, only increase the number if data pool is constantly empty which may potentially affect the publishing performance
  - `numWorkers` better to keep this same as `numPublishers`, this only pull the data from the pool and fill the data chanel, which is dedicated for each publisher

1. init the project (you only need to do that once)

```shell
make init
```

2. build the Golang code

```shell
make build
```

3. Dump the data
You should be able see a binary named `main` in the project root directory,
simply run it `./main` or `make run` 

`Ctr + C` to stop it

