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
	"github.com/shenqianjin/soften-client-go/soften/support/util"
	"github.com/stretchr/testify/assert"
)

var (
	DefaultPulsarUrl     = "pulsar://localhost:6650"
	DefaultPulsarHttpUrl = "http://localhost:8080"

	DefaultTopic           = "my-topic"
	PrefixTestProduce      = "test-produce"
	PrefixTestProduceBatch = "test-produce-batch"
	PrefixTestConsume      = "test-consume"
	PrefixTestConsumeLimit = "test-consume-limit"
	PrefixTestConsumeBatch = "test-consume-batch"
	PrefixTestConsumeCheck = "test-consume-check"
	PrefixTestListen       = "test-listen"

	Size64 = 64
	Size1K = 1024
	/*size2K = Size1K * 2
	size3K = Size1K * 3
	size4K = Size1K * 4
	size5K = Size1K * 5
	size1M = Size1K * 1024
	size1G = size1M * 1024*/

	TimeFormat = "20060102150405"

	topicIndex uint32 = 1
)

func GenerateTestTopic(prefix string) string {
	index := atomic.AddUint32(&topicIndex, 1)
	now := time.Now().Format(TimeFormat)
	topic := strings.Join([]string{prefix, now, strconv.Itoa(int(index))}, "-")
	return topic
}

func TestSubscriptionName() string {
	return "testSub"
}

func TestSubscriptionPrefix() string {
	return "-" + TestSubscriptionName()
}

func FormatStatusTopic(topic, subscription string, levelSuffix, statusSuffix string) string {
	return topic + levelSuffix + "-" + subscription + statusSuffix
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
		topic = GenerateTestTopic(DefaultTopic)
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
		conf.Topic = GenerateTestTopic(DefaultTopic)
	}
	if conf.SubscriptionName == "" {
		conf.SubscriptionName = TestSubscriptionName()
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

func CleanUpTopics(t *testing.T, manager admin.RobustTopicManager, topics ...string) {
	if len(topics) == 0 {
		return
	}
	for _, topic := range topics {
		CleanUpTopic(t, manager, topic)
	}
}

func CleanUpTopic(t *testing.T, manager admin.RobustTopicManager, storedTopic string) {
	if storedTopic == "" {
		return
	}
	err := manager.Delete(storedTopic)
	assert.True(t, err == nil || strings.Contains(err.Error(), "404 Not Found"), err)
}

type CreateTopicOptions struct {
	Partitions   uint
	Subscription string
	Levels       []string
	Statuses     []string
}

func CreateTopicIfNotFound(t *testing.T, manager admin.RobustTopicManager, groundTopic string, options CreateTopicOptions) {
	topics, err := util.FormatTopics(groundTopic, options.Levels, options.Statuses, options.Subscription)
	assert.Nil(t, err)
	CreateTopicsIfNotFound(t, manager, topics, options.Partitions)

}

func CreateTopicsIfNotFound(t *testing.T, manager admin.RobustTopicManager, topics []string, partitions uint) {
	for _, topic := range topics {
		if topic == "" {
			continue
		}
		_, err := manager.Stats(topic)
		if err != nil && strings.Contains(err.Error(), "404 Not Found") {
			err1 := manager.Create(topic, partitions)
			assert.Nil(t, err1)
			continue
		}
		assert.Nil(t, err)
	}
}
