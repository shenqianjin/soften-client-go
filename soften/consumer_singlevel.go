package soften

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

type singleLeveledConsumer struct {
	logger           log.Logger
	messageCh        chan consumerMessage // channel used to deliver message to application
	level            internal.TopicLevel
	statusStrategy   internal.BalanceStrategy // 消费策略
	mainConsumer     *statusConsumer
	pendingConsumer  *statusConsumer
	blockingConsumer *statusConsumer
	retryingConsumer *statusConsumer
	closeOnce        sync.Once
	closeCh          chan struct{}
	metricsProvider  *internal.MetricsProvider
	metricsTopics    string
	metricsLevels    string
}

func newSingleLeveledConsumer(parentLogger log.Logger, client *client, level internal.TopicLevel, conf *config.ConsumerConfig,
	messageCh chan consumerMessage, deciders *leveledConsumeDeciders) (*singleLeveledConsumer, error) {
	groundTopic := conf.Topic
	subscription := conf.SubscriptionName
	metricsTopics := internal.TopicParser.FormatListAsAbbr(conf.Topics)
	metricsLevels := internal.TopicLevelParser.FormatList(conf.Levels)
	slc := &singleLeveledConsumer{logger: parentLogger.SubLogger(log.Fields{"level": level}),
		level: level, messageCh: messageCh, statusStrategy: conf.BalanceStrategy,
		closeCh:       make(chan struct{}),
		metricsTopics: metricsTopics, metricsLevels: metricsLevels,
		metricsProvider: client.metricsProvider}
	// create status singleLeveledConsumer
	if mainConsumer, mainTopic, err := slc.internalSubscribe(client, conf, level, message.StatusReady); err != nil {
		return nil, err
	} else {
		metrics := client.metricsProvider.GetListenerConsumerMetrics(groundTopic, level, message.StatusReady,
			subscription, mainTopic)
		statusReadyPolicy := &config.StatusPolicy{ConsumeWeight: conf.Ready.ConsumeWeight}
		options := statusConsumerOptions{level: level, status: message.StatusReady, policy: statusReadyPolicy, metrics: metrics}
		slc.mainConsumer = newStatusConsumer(slc.logger, mainConsumer, options, nil)
	}
	if *conf.PendingEnable {
		if pendingConsumer, pendingTopic, err := slc.internalSubscribe(client, conf, level, message.StatusPending); err != nil {
			return nil, err
		} else {
			metrics := client.metricsProvider.GetListenerConsumerMetrics(groundTopic, level, message.StatusPending,
				subscription, pendingTopic)
			options := statusConsumerOptions{level: level, status: message.StatusPending, policy: conf.Pending, metrics: metrics}
			slc.pendingConsumer = newStatusConsumer(slc.logger, pendingConsumer, options, deciders.pendingDecider)
		}
	}
	if *conf.BlockingEnable {
		if blockingConsumer, blockingTopic, err := slc.internalSubscribe(client, conf, level, message.StatusBlocking); err != nil {
			return nil, err
		} else {
			metrics := client.metricsProvider.GetListenerConsumerMetrics(groundTopic, level, message.StatusBlocking,
				subscription, blockingTopic)
			options := statusConsumerOptions{level: level, status: message.StatusBlocking, policy: conf.Blocking, metrics: metrics}
			slc.blockingConsumer = newStatusConsumer(slc.logger, blockingConsumer, options, deciders.blockingDecider)
		}
	}
	if *conf.RetryingEnable {
		if retryingConsumer, retryingTopic, err := slc.internalSubscribe(client, conf, level, message.StatusRetrying); err != nil {
			return nil, err
		} else {
			metrics := client.metricsProvider.GetListenerConsumerMetrics(groundTopic, level, message.StatusRetrying,
				subscription, retryingTopic)
			options := statusConsumerOptions{level: level, status: message.StatusRetrying, policy: conf.Retrying, metrics: metrics}
			slc.retryingConsumer = newStatusConsumer(slc.logger, retryingConsumer, options, deciders.retryingDecider)
		}
	}
	// start to listen message from all status singleLeveledConsumer
	go slc.retrieveStatusMessages()
	slc.logger.Info("subscribed leveled consumer")
	return slc, nil
}

func (slc *singleLeveledConsumer) retrieveStatusMessages() {
	chs := make([]<-chan consumerMessage, 1)
	weights := make([]uint, 1)
	chs[0] = slc.mainConsumer.StatusChan()
	weights[0] = slc.mainConsumer.policy.ConsumeWeight
	if slc.retryingConsumer != nil {
		chs = append(chs, slc.retryingConsumer.StatusChan())
		weights = append(weights, slc.retryingConsumer.policy.ConsumeWeight)
	}
	if slc.pendingConsumer != nil {
		chs = append(chs, slc.pendingConsumer.StatusChan())
		weights = append(weights, slc.pendingConsumer.policy.ConsumeWeight)
	}
	if slc.blockingConsumer != nil {
		chs = append(chs, slc.blockingConsumer.StatusChan())
		weights = append(weights, slc.blockingConsumer.policy.ConsumeWeight)
	}
	balanceStrategy, err := config.BuildStrategy(slc.statusStrategy, weights)
	if err != nil {
		panic(fmt.Errorf("failed to start retrieve: %v", err))
	}
	for {
		msg, ok := messageChSelector.receiveOneByWeight(chs, balanceStrategy, &[]int{})
		if !ok {
			slc.logger.Warnf("status chan closed")
			break
		}

		// 获取到消息
		select {
		case <-slc.closeCh:
			slc.logger.Errorf("received message failed because consumer closed. mid: %v", msg.ID())
			return
		default:
			slc.messageCh <- msg
		}
	}
}

// Chan returns a channel to consume messages from
func (slc *singleLeveledConsumer) Chan() <-chan consumerMessage {
	return slc.messageCh
}

func (slc *singleLeveledConsumer) Close() {
	slc.closeOnce.Do(func() {
		close(slc.closeCh)
		slc.mainConsumer.Close()
		if slc.blockingConsumer != nil {
			slc.blockingConsumer.Close()
		}
		if slc.pendingConsumer != nil {
			slc.pendingConsumer.Close()
		}
		if slc.retryingConsumer != nil {
			slc.retryingConsumer.Close()
		}
	})
	slc.logger.Info("closed leveled consumer")
}

func (slc *singleLeveledConsumer) internalSubscribe(cli *client, conf *config.ConsumerConfig, lvl internal.TopicLevel, status internal.MessageStatus) (pulsar.Consumer, string, error) {
	groundTopic := conf.Topic
	subscriptionSuffix := "-" + conf.SubscriptionName
	levelSuffix := lvl.TopicSuffix()
	statusSuffix := status.TopicSuffix()
	var topic string
	switch status {
	case message.StatusReady:
		topic = groundTopic + levelSuffix + statusSuffix
	case message.StatusRetrying:
		topic = groundTopic + levelSuffix + subscriptionSuffix + statusSuffix
	case message.StatusPending:
		topic = groundTopic + levelSuffix + subscriptionSuffix + statusSuffix
	case message.StatusBlocking:
		topic = groundTopic + levelSuffix + subscriptionSuffix + statusSuffix
	default:
		return nil, "", errors.New(fmt.Sprintf("subsribe on invalid status: %v", status))
	}
	consumerOption := pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            conf.SubscriptionName,
		Type:                        conf.Type,
		SubscriptionInitialPosition: conf.SubscriptionInitialPosition,
		NackRedeliveryDelay:         time.Second * time.Duration(conf.NackRedeliveryDelay),
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
	if status == message.StatusReady && lvl == conf.Level {
		consumerOption.Topics = conf.Topics
		consumerOption.RetryEnable = conf.RetryEnable
	} else {
		consumerOption.Topics = nil
		consumerOption.RetryEnable = false
	}
	pulsarConsumer, err := cli.Client.Subscribe(consumerOption)
	return pulsarConsumer, topic, err
}
