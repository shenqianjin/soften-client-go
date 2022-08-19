package internal

import (
	"crypto/rand"
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/stretchr/testify/assert"
)

var (
	DefaultPulsarUrl     = "pulsar://localhost:6650"
	DefaultPulsarHttpUrl = "http://localhost:8080"

	DefaultTopic = "my-topic"

	Size64 = 64
	Size1K = 1024
	/*size2K = Size1K * 2
	size3K = Size1K * 3
	size4K = Size1K * 4
	size5K = Size1K * 5
	size1M = Size1K * 1024
	size1G = size1M * 1024*/

	TimeFormat = "20060102150405"

	topicCount = int32(0)
)

func GenerateTestTopic() string {
	index := atomic.AddInt32(&topicCount, 1)
	now := time.Now().Format(TimeFormat)
	return strings.Join([]string{DefaultTopic, now, strconv.Itoa(int(index))}, "-")
}

func GenerateSubscribeName() string {
	return GenerateSubscribeNameByTopic(GenerateTestTopic())
}

func GenerateSubscribeNameByTopic(topic string) string {
	return topic + "-sub"
}

func NewClient(url string) soften.Client {
	if url == "" {
		url = DefaultPulsarUrl
	}
	client, err := soften.NewClient(config.ClientConfig{
		URL: url,
		//ConnectionTimeout: 1,
		//OperationTimeout:  120,
	})
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func CreateProducer(client soften.Client, topic string) soften.Producer {
	if topic == "" {
		topic = GenerateTestTopic()
	}
	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic: topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	return producer
}

func CreateListener(client soften.Client, conf config.ConsumerConfig) soften.Listener {
	if conf.Topic == "" {
		conf.Topic = GenerateTestTopic()
	}
	if conf.SubscriptionName == "" {
		conf.SubscriptionName = GenerateSubscribeNameByTopic(conf.Topic)
	}
	listener, err := client.CreateListener(conf)
	if err != nil {
		log.Fatal(err)
	}
	return listener
}

func GenerateProduceMessage(size int, kvs ...string) *pulsar.ProducerMessage {
	if size < 0 {
		size = Size1K
	}
	data := make([]byte, size)
	_, _ = rand.Read(data)
	properties := make(map[string]string)
	for k := 0; k < len(kvs)-1; k += 2 {
		properties[kvs[k]] = kvs[k+1]
	}
	return &pulsar.ProducerMessage{
		Payload:    data,
		Properties: properties,
	}
}

func CleanUpTopic(t *testing.T, manager admin.TopicManager, storedTopic string) {
	if storedTopic == "" {
		return
	}
	err := manager.Delete(storedTopic)
	assert.True(t, err == nil || strings.Contains(err.Error(), "404 Not Found"), err)
}
