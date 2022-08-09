package soften

import (
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

type leveledConsumer struct {
	logger           log.Logger
	messageCh        chan ConsumerMessage // channel used to deliver message to application
	level            internal.TopicLevel
	statusStrategy   internal.BalanceStrategy // 消费策略
	mainConsumer     *statusConsumer
	pendingConsumer  *statusConsumer
	blockingConsumer *statusConsumer
	retryingConsumer *statusConsumer
}

func newSingleLeveledConsumer(parentLogger log.Logger, client *client, level internal.TopicLevel, conf *config.ConsumerConfig,
	messageCh chan ConsumerMessage, deciders *leveledConsumeDeciders) (*leveledConsumer, error) {
	cs := &leveledConsumer{logger: parentLogger.SubLogger(log.Fields{"level": level}),
		level: level, messageCh: messageCh, statusStrategy: conf.BalanceStrategy}
	// create status leveledConsumer
	if mainConsumer, err := cs.internalSubscribe(client, conf, level, message.StatusReady); err != nil {
		return nil, err
	} else {
		cs.mainConsumer = newStatusConsumer(cs.logger, mainConsumer, message.StatusReady, conf.Ready, nil)
	}
	if conf.PendingEnable {
		if pendingConsumer, err := cs.internalSubscribe(client, conf, level, message.StatusPending); err != nil {
			return nil, err
		} else {
			cs.pendingConsumer = newStatusConsumer(cs.logger, pendingConsumer, message.StatusPending, conf.Pending, deciders.pendingDecider)
		}
	}
	if conf.BlockingEnable {
		if blockingConsumer, err := cs.internalSubscribe(client, conf, level, message.StatusBlocking); err != nil {
			return nil, err
		} else {
			cs.blockingConsumer = newStatusConsumer(cs.logger, blockingConsumer, message.StatusBlocking, conf.Blocking, deciders.blockingDecider)
		}
	}
	if conf.RetryingEnable {
		if retryingConsumer, err := cs.internalSubscribe(client, conf, level, message.StatusRetrying); err != nil {
			return nil, err
		} else {
			cs.retryingConsumer = newStatusConsumer(cs.logger, retryingConsumer, message.StatusRetrying, conf.Retrying, deciders.retryingDecider)
		}
	}
	// start to listen message from all status leveledConsumer
	go cs.retrieveStatusMessages()
	cs.logger.Info("subscribed leveled consumer")
	return cs, nil
}

func (msc *leveledConsumer) retrieveStatusMessages() {
	chs := make([]<-chan ConsumerMessage, 1)
	weights := make([]uint, 1)
	chs[0] = msc.mainConsumer.StatusChan()
	weights[0] = msc.mainConsumer.policy.ConsumeWeight
	if msc.retryingConsumer != nil {
		chs = append(chs, msc.retryingConsumer.StatusChan())
		weights = append(weights, msc.retryingConsumer.policy.ConsumeWeight)
	}
	if msc.pendingConsumer != nil {
		chs = append(chs, msc.pendingConsumer.StatusChan())
		weights = append(weights, msc.pendingConsumer.policy.ConsumeWeight)
	}
	if msc.blockingConsumer != nil {
		chs = append(chs, msc.blockingConsumer.StatusChan())
		weights = append(weights, msc.blockingConsumer.policy.ConsumeWeight)
	}
	balanceStrategy, err := config.BuildStrategy(msc.statusStrategy, weights)
	if err != nil {
		panic(fmt.Errorf("failed to start retrieve: %v", err))
	}
	for {
		msg, ok := messageChSelector.receiveOneByWeight(chs, balanceStrategy, &[]int{})
		if !ok {
			msc.logger.Warnf("status chan closed")
			break
		}

		// 获取到消息
		if msg.Message != nil && msg.Consumer != nil {
			//fmt.Printf("received msg  msgId: %v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
			msg.LeveledMessage = &leveledMessage{level: msc.level}
			msc.messageCh <- msg
		} else {
			panic(fmt.Sprintf("consumed an invalid msg: %v", msg))
		}
	}
}

// Chan returns a channel to consume messages from
func (msc *leveledConsumer) Chan() <-chan ConsumerMessage {
	return msc.messageCh
}

func (msc *leveledConsumer) Close() {
	msc.mainConsumer.Close()
	if msc.blockingConsumer != nil {
		msc.blockingConsumer.Close()
	}
	if msc.pendingConsumer != nil {
		msc.pendingConsumer.Close()
	}
	if msc.retryingConsumer != nil {
		msc.retryingConsumer.Close()
	}
	close(msc.messageCh)
	msc.logger.Info("closed leveled consumer")
}

func (msc *leveledConsumer) internalSubscribe(cli *client, conf *config.ConsumerConfig, lvl internal.TopicLevel, status internal.MessageStatus) (pulsar.Consumer, error) {
	levelSuffix := lvl.TopicSuffix()
	statusSuffix := status.TopicSuffix()
	topic := conf.Topics[0] + levelSuffix + statusSuffix
	subscriptionName := conf.SubscriptionName + levelSuffix + statusSuffix
	consumerOption := pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subscriptionName,
		Type:                        conf.Type,
		SubscriptionInitialPosition: conf.SubscriptionInitialPosition,
		NackRedeliveryDelay:         conf.NackRedeliveryDelay,
		NackBackoffPolicy:           conf.NackBackoffPolicy,
		MessageChannel:              nil,
	}
	if conf.DLQ != nil {
		consumerOption.DLQ = &pulsar.DLQPolicy{
			MaxDeliveries:    conf.DLQ.MaxDeliveries,
			RetryLetterTopic: conf.DLQ.RetryLetterTopic,
			DeadLetterTopic:  conf.DLQ.DeadLetterTopic,
		}
	}
	// only main status need compatible with pulsar retry enable and multi-topics
	if status == message.StatusReady {
		consumerOption.Topics = conf.Topics
		consumerOption.RetryEnable = conf.RetryEnable
	} else {
		consumerOption.Topics = nil
		consumerOption.RetryEnable = false
	}
	pulsarConsumer, err := cli.Client.Subscribe(consumerOption)
	return pulsarConsumer, err
}
