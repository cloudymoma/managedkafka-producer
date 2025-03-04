package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type IPDetail struct {
	Accuracy  string `json:"accuracy"`
	Adcode    string `json:"adcode"`
	Areacode  string `json:"areacode"`
	Asnumber  string `json:"asnumber"`
	City      string `json:"city"`
	Continent string `json:"continent"`
	Country   string `json:"country"`
	Isp       string `json:"isp"`
	Latwgs    string `json:"latwgs"`
	Lngwgs    string `json:"lngwgs"`
	Owner     string `json:"owner"`
	Province  string `json:"province"`
	Radius    string `json:"radius"`
	Source    string `json:"source"`
	Timezone  string `json:"timezone"`
	Zipcode   string `json:"zipcode"`
}

type Model struct {
	Type           string `json:"type"`
	UUID           string `json:"uuid"`
	DistinctID     string `json:"distinct_id"`
	EventName      string `json:"event_name"`
	Time           int64  `json:"time"`
	ZoneOffset     int    `json:"zone_offset"`
	NetworkType    string `json:"network_type"`
	Carrier        string `json:"carrier"`
	AppVersion     string `json:"app_version"`
	OSVersion      string `json:"os_version"`
	LibVersion     string `json:"lib_version"`
	SystemLanguage string `json:"system_language"`
	CPU            string `json:"cpu"`
	RAM            string `json:"ram"`
	Disk           string `json:"disk"`
	FPS            int    `json:"fps"`
	Properties     string `json:"properties"`
}

type MyData struct {
	DeviceID     string   `json:"device_id"`
	AppID        string   `json:"app_id"`
	InstallTime  int64    `json:"install_time"`
	OS           string   `json:"os"`
	ScreenWidth  int      `json:"screen_width"`
	ScreenHeight int      `json:"screen_height"`
	DeviceModel  string   `json:"device_model"`
	DeviceType   string   `json:"device_type"`
	BundleID     string   `json:"bundle_id"`
	Manufacturer string   `json:"manufacturer"`
	Stime        int64    `json:"stime"`
	IP           string   `json:"ip"`
	IPDetail     IPDetail `json:"ip_detail"`
	Model        []Model  `json:"model"`
}

// dataPool with a counter.
type countedDataPool struct {
	pool  sync.Pool
	count int64 // Use an atomic counter
}

func (p *countedDataPool) Get() *MyData {
	if p.count == 0 {
		return datagen()
	}

	atomic.AddInt64(&p.count, -1) // Decrement count when taking from pool
	return p.pool.Get().(*MyData)
}

func (p *countedDataPool) Put(data *MyData) {
	p.pool.Put(data)
	atomic.AddInt64(&p.count, 1) // Increment count when returning to pool
}

func (p *countedDataPool) Len() int64 {
	return atomic.LoadInt64(&p.count) // Non-blocking read of the counter
}

var dataPool = countedDataPool{
	pool: sync.Pool{
		New: func() any { return &MyData{} },
	},
	count: 0, // Explicitly initialize count to 0.
}

// Global counter for total messages published
var totalMessagesPublished int64 = 0

func datagen() *MyData {
	// Get a MyData object from the pool, or create a new one if the pool is empty.
	data := &MyData{}

	data.DeviceID = randomString(36)
	data.AppID = randomString(32)
	data.InstallTime = rand.Int63()
	data.OS = "iOS"
	data.ScreenWidth = rand.Intn(1000)
	data.ScreenHeight = rand.Intn(2000)
	data.DeviceModel = "iPhone15,5"
	data.DeviceType = "iPhone"
	data.BundleID = "com.example.app"
	data.Manufacturer = "Apple"
	data.Stime = rand.Int63()
	data.IP = "192.168.1.1"
	data.IPDetail = IPDetail{
		Accuracy:  "城市",
		Adcode:    "",
		Areacode:  "GB",
		Asnumber:  "5089",
		City:      "伦敦",
		Continent: "欧洲",
		Country:   "英国",
		Isp:       "维珍传媒有限公司",
		Latwgs:    "51.513816",
		Lngwgs:    "-0.121887",
		Owner:     "JARROW",
		Province:  "英格兰",
		Radius:    "",
		Source:    "数据挖掘",
		Timezone:  "UTC+0",
		Zipcode:   "WC2B 5QZ",
	}
	data.Model = []Model{
		{
			Type:           "track",
			UUID:           randomString(36),
			DistinctID:     randomString(36),
			EventName:      "app_start_detail",
			Time:           rand.Int63(),
			ZoneOffset:     1,
			NetworkType:    "WIFI",
			Carrier:        "--",
			AppVersion:     "4.6.7",
			OSVersion:      "18.0.1",
			LibVersion:     "1.0.0.0",
			SystemLanguage: "en",
			CPU:            "arm64e",
			RAM:            "1.0/5.5",
			Disk:           "15.8/119.1",
			FPS:            50,
			Properties:     `{"#device_id":"B4FB831D-2C2E-4A16-9D28-2CD927F565BC","#lib":"iOS","time":1728489599.611932,"s_coldstratnum_V421":196,"session_id":"AE5C57D2-C78E-4C6B-AA18-CDE2421E4A79","operation_type":3,"#simulator":false,"data_ad_waynum":{"1":"4337","aggregtion":"IS","pici":"43-2"}}`,
		},
	}

	return data
}

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// addCommas adds commas to an integer string.
func addCommas(n int64) string {
	in := fmt.Sprintf("%d", n)
	var out []byte
	l := len(in)
	for i := 0; i < l; i++ {
		if i > 0 && (l-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, in[i])
	}
	return string(out)
}

func main() {
	// Replace with your project ID and topic ID
	// projectID := "du-hast-mich"
	// serviceAccountKeyFilePath := "/usr/local/google/home/binwu/workspace/google/sa.json"
	// region := "us-central1"
	// clusterName := "dingo-kafka"
	topicName := "dingo-topic"

	// Create a new Pub/Sub client.
	// ctx := context.Background()

	// https://github.com/googleapis/managedkafka/blob/eee84856cc5e27e27c7041da2eead03cba71e019/README.md
	kafkaProducer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                   "bootstrap.dingo-kafka.us-central1.managedkafka.du-hast-mich.cloud.goog:9092",
		"security.protocol":                   "SASL_SSL",
		"sasl.mechanisms":                     "OAUTHBEARER",
		"sasl.oauthbearer.token.endpoint.url": "localhost:14293",
		"sasl.oauthbearer.client.id":          "unused",
		"sasl.oauthbearer.client.secret":      "unused",
		"sasl.oauthbearer.method":             "oidc",
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer kafkaProducer.Close()

	// Delivery report handler (goroutine for handling delivery reports)
	go func() {
		for e := range kafkaProducer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					// fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
					atomic.AddInt64(&totalMessagesPublished, 1) // Increment on successful delivery
				}
			case kafka.Error:
				log.Printf("Kafka Error: %v\n", ev)
			}
		}
	}()

	// Status reporting goroutine (non-blocking)
	go func() {
		for {
			poolLen := dataPool.Len()
			totalSent := atomic.LoadInt64(&totalMessagesPublished) // Non-blocking read

			// log.Printf("DataPool size: %d, Total messages published: %d", poolLen, totalSent)
			log.Printf("DataPool size: %s, Total messages published: %s", addCommas(poolLen), addCommas(totalSent))

			time.Sleep(3 * time.Second)
		}
	}()

	// numPublishers := runtime.NumCPU()     // threads for publishing message to Kafka from channels
	// numDataGenThreads := runtime.NumCPU() // threads for generating data and fill the dataPool
	// numWorkers := runtime.NumCPU()        // threads for consuming data from dataPool and publish to channels
	numPublishers := 2
	numDataGenThreads := 2
	numWorkers := 2

	// Prefill the dataPool before starting the publishers
	const poolSize = 999990

	const prefillSize = poolSize / 10
	log.Printf("Prefilling the dataPool with %s items...", addCommas(prefillSize))
	for i := 0; i < prefillSize; i++ {
		dataPool.Put(datagen())
	}
	log.Printf("DataPool prefill complete.  Current size: %s", addCommas(dataPool.Len()))

	const bufferSize = 99999

	// Create a dedicated channel for EACH publisher.
	publisherChs := make([]chan *kafka.Message, numPublishers)
	for i := 0; i < numPublishers; i++ {
		publisherChs[i] = make(chan *kafka.Message, bufferSize)
	}

	// Data Generation Goroutines
	var wgDataGen sync.WaitGroup
	wgDataGen.Add(numDataGenThreads)
	for i := 0; i < numDataGenThreads; i++ {
		go func(threadID int) {
			defer wgDataGen.Done()
			for {
				// Check if the pool is full
				if dataPool.Len() >= poolSize { // Pool is full
					log.Printf("Thread %d: DataPool is full, current size: %s, Waiting...",
						threadID, addCommas(dataPool.Len()))
					time.Sleep(1000 * time.Millisecond) // Pause briefly
					continue                            // Skip this iteration
				}

				// Put the item into the pool.
				dataPool.Put(datagen())
			}
		}(i)
	}

	// Data Consumption and fill channels Goroutines
	var wgWorkers sync.WaitGroup
	wgWorkers.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func(publisherChID int, ch chan *kafka.Message) {
			defer wgWorkers.Done()
			for { // Infinite loop to continuously consume from dataPool and produce to Kafka
				item := dataPool.Get()

				jsonData, err := json.Marshal(item)
				if err != nil {
					log.Printf("Publisher %d: Failed to marshal JSON: %v", publisherChID, err)
					continue
				}

				msg := &kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
					Value:          jsonData,
				}

				// Non-blocking send to Kafka channel with retry on full queue
				select {
				case ch <- msg: // Try to send to Kafka
				default:
					// Kafka channel is full.  Wait and retry.
					log.Println("Publisher channel is full, waiting...")
					time.Sleep(1 * time.Second)
					ch <- msg // Requeue on the *same* channel.
				}
			}
		}(i, publisherChs[i]) // Pass the dedicated channel
	}

	// Kafka Producer Goroutines
	var wgPublishers sync.WaitGroup
	wgPublishers.Add(numPublishers)
	for i := 0; i < numPublishers; i++ {
		go func(publisherID int, ch chan *kafka.Message) {
			defer wgPublishers.Done()
			for msg := range ch {
				err = kafkaProducer.Produce(msg, nil)
				if err != nil {
					if err.(kafka.Error).Code() == kafka.ErrQueueFull {
						// fmt.Println("Producer queue is full, waiting...")
						log.Printf("Publisher %d: Kafka producer queue full: %v", publisherID, err)
						time.Sleep(1 * time.Second)
						// Important:  Re-send on the SAME channel.
						ch <- msg // Requeue on the *same* channel.
						continue
					}
					log.Printf("Failed to produce message: %v\n", err)
				}
				time.Sleep(100 * time.Millisecond) // to avoid hitting the rate limit
			}
		}(i, publisherChs[i]) // Pass the dedicated channel to each publisher
	}

	// Handle SIGINT and SIGTERM for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan // Block until a signal is received
		log.Printf("Received signal: %v. Shutting down...", sig)

		// Close all publisher channels.  This signals the publisher goroutines to exit.
		for _, ch := range publisherChs {
			close(ch)
		}

		// Wait for data generation to finish
		wgDataGen.Wait()
		// Wait for publishers to finish.
		wgPublishers.Wait()
		wgWorkers.Wait()

		kafkaProducer.Flush(15 * 1000) // Flush any remaining messages.

		log.Println("Graceful shutdown complete.")
		os.Exit(0)
	}()

	// Wait for data generation to finish
	wgDataGen.Wait()
	// Wait for publishers to finish.
	wgPublishers.Wait()
	wgWorkers.Wait()

	kafkaProducer.Flush(15 * 1000) // Flush any remaining messages.

	log.Println("Finished publishing messages (interrupted).")
	os.Exit(0)
}
