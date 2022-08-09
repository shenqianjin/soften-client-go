package producer

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
)

func TestSend_1Msg(t *testing.T) {
	topic := internal.GenerateTestTopic()
	manager := admin.NewAdminManager(internal.DefaultPulsarHttpUrl)

	internal.CleanUpTopic(t, manager, topic)
	defer func() {
		internal.CleanUpTopic(t, manager, topic)
	}()

	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()

	producer := internal.CreateProducer(client, topic)
	defer producer.Close()

	msgID, err := producer.Send(context.Background(), internal.GenerateProduceMessage(internal.Size1K))
	assert.Nil(t, err)
	fmt.Println(msgID)

	stats, err := manager.Stats(topic)
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgInCounter)
}

func TestSendAsync_1Msg(t *testing.T) {
	topic := internal.GenerateTestTopic()
	manager := admin.NewAdminManager(internal.DefaultPulsarHttpUrl)

	internal.CleanUpTopic(t, manager, topic)
	defer func() {
		internal.CleanUpTopic(t, manager, topic)
	}()

	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()

	producer := internal.CreateProducer(client, topic)
	defer producer.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)
	producer.SendAsync(context.Background(), internal.GenerateProduceMessage(internal.Size1K),
		func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
			fmt.Println("sent async message: ", id)
			wg.Done()
		})
	wg.Wait()

	stats, err := manager.Stats(topic)
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgInCounter)
}
