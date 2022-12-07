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
	"github.com/shenqianjin/soften-client-go/soften/internal/concurrencylimit"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"go.uber.org/ratelimit"
)

type singleLeveledConsumer struct {
	logger             log.Logger
	rateLimiter        ratelimit.Limiter
	concurrencyLimiter concurrencylimit.Limiter
	messageCh          chan consumerMessage // channel used to deliver message to application
	level              internal.TopicLevel
	statusStrategy     internal.BalanceStrategy // 消费策略
	readyConsumer      *statusConsumer
	pendingConsumer    *statusConsumer
	blockingConsumer   *statusConsumer
	retryingConsumer   *statusConsumer
	closeOnce          sync.Once
	closeCh            chan struct{}
	metricsProvider    *internal.MetricsProvider
	metricsTopics      string
}

type singleLeveledConsumerOptions struct {
	Topics                      []string
	SubscriptionName            string
	Type                        pulsar.SubscriptionType
	SubscriptionInitialPosition pulsar.SubscriptionInitialPosition
	NackBackoffDelayPolicy      pulsar.NackBackoffPolicy
	NackRedeliveryDelay         uint
	RetryEnable                 bool
	DLQ                         *config.DLQPolicy

	BalanceStrategy internal.BalanceStrategy
	MainLevel       internal.TopicLevel

	policy *config.LevelPolicy
}

func newSingleLeveledConsumer(parentLogger log.Logger, client *client, level internal.TopicLevel, options *singleLeveledConsumerOptions,
	messageCh chan consumerMessage, deciders *leveledConsumeDeciders) (*singleLeveledConsumer, error) {
	groundTopic := options.Topics[0]
	subscription := options.SubscriptionName
	slc := &singleLeveledConsumer{logger: parentLogger.SubLogger(log.Fields{"level": level}),
		level: level, messageCh: messageCh, statusStrategy: options.BalanceStrategy,
		closeCh:         make(chan struct{}),
		metricsProvider: client.metricsProvider}
	// create status singleLeveledConsumer
	if readyConsumer, mainTopic, err := slc.internalSubscribe(client, options, level, message.StatusReady); err != nil {
		return nil, err
	} else {
		metrics := client.metricsProvider.GetListenerConsumerMetrics(groundTopic, level, message.StatusReady,
			subscription, mainTopic)
		statusReadyPolicy := &config.StatusPolicy{ConsumeWeight: options.policy.Ready.ConsumeWeight, ConsumeLimit: options.policy.Ready.ConsumeLimit}
		options := statusConsumerOptions{level: level, status: message.StatusReady, policy: statusReadyPolicy, metrics: metrics}
		slc.readyConsumer = newStatusConsumer(slc.logger, readyConsumer, options, nil)
	}
	if *options.policy.PendingEnable {
		if pendingConsumer, pendingTopic, err := slc.internalSubscribe(client, options, level, message.StatusPending); err != nil {
			return nil, err
		} else {
			metrics := client.metricsProvider.GetListenerConsumerMetrics(groundTopic, level, message.StatusPending,
				subscription, pendingTopic)
			options := statusConsumerOptions{level: level, status: message.StatusPending, policy: options.policy.Pending, metrics: metrics}
			slc.pendingConsumer = newStatusConsumer(slc.logger, pendingConsumer, options, deciders.pendingDecider)
		}
	}
	if *options.policy.BlockingEnable {
		if blockingConsumer, blockingTopic, err := slc.internalSubscribe(client, options, level, message.StatusBlocking); err != nil {
			return nil, err
		} else {
			metrics := client.metricsProvider.GetListenerConsumerMetrics(groundTopic, level, message.StatusBlocking,
				subscription, blockingTopic)
			options := statusConsumerOptions{level: level, status: message.StatusBlocking, policy: options.policy.Blocking, metrics: metrics}
			slc.blockingConsumer = newStatusConsumer(slc.logger, blockingConsumer, options, deciders.blockingDecider)
		}
	}
	if *options.policy.RetryingEnable {
		if retryingConsumer, retryingTopic, err := slc.internalSubscribe(client, options, level, message.StatusRetrying); err != nil {
			return nil, err
		} else {
			metrics := client.metricsProvider.GetListenerConsumerMetrics(groundTopic, level, message.StatusRetrying,
				subscription, retryingTopic)
			options := statusConsumerOptions{level: level, status: message.StatusRetrying, policy: options.policy.Retrying, metrics: metrics}
			slc.retryingConsumer = newStatusConsumer(slc.logger, retryingConsumer, options, deciders.retryingDecider)
		}
	}
	if *options.policy.ConsumeLimit.MaxOPS > 0 {
		slc.rateLimiter = ratelimit.New(int(*options.policy.ConsumeLimit.MaxOPS))
	}
	if *options.policy.ConsumeLimit.MaxConcurrency > 0 {
		slc.concurrencyLimiter = concurrencylimit.New(int(*options.policy.ConsumeLimit.MaxConcurrency))
	}
	// start to listen message from all status singleLeveledConsumer
	go slc.retrieveStatusMessages()
	slc.logger.Info("subscribed leveled consumer")
	return slc, nil
}

func (slc *singleLeveledConsumer) retrieveStatusMessages() {
	chs := make([]<-chan consumerMessage, 1)
	weights := make([]uint, 1)
	chs[0] = slc.readyConsumer.StatusChan()
	weights[0] = *slc.readyConsumer.policy.ConsumeWeight
	if slc.retryingConsumer != nil {
		chs = append(chs, slc.retryingConsumer.StatusChan())
		weights = append(weights, *slc.retryingConsumer.policy.ConsumeWeight)
	}
	if slc.pendingConsumer != nil {
		chs = append(chs, slc.pendingConsumer.StatusChan())
		weights = append(weights, *slc.pendingConsumer.policy.ConsumeWeight)
	}
	if slc.blockingConsumer != nil {
		chs = append(chs, slc.blockingConsumer.StatusChan())
		weights = append(weights, *slc.blockingConsumer.policy.ConsumeWeight)
	}
	balanceStrategy, err := config.BuildStrategy(slc.statusStrategy, weights)
	if err != nil {
		panic(fmt.Errorf("failed to start retrieve: %v", err))
	}
	for {
		// retrieve message for current level
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
		}
		// check rate limit
		if slc.rateLimiter != nil {
			slc.rateLimiter.Take()
		}
		// check concurrency limit
		if slc.concurrencyLimiter != nil {
			slc.concurrencyLimiter.Acquire()
			if originalDeferFunc := msg.internalExtra.deferFunc; originalDeferFunc != nil {
				msg.internalExtra.deferFunc = func() { originalDeferFunc(); slc.concurrencyLimiter.Release() }
			} else {
				msg.internalExtra.deferFunc = func() { slc.concurrencyLimiter.Release() }
			}
		}
		// delivery
		slc.messageCh <- msg
	}
}

// Chan returns a channel to consume messages from
func (slc *singleLeveledConsumer) Chan() <-chan consumerMessage {
	return slc.messageCh
}

func (slc *singleLeveledConsumer) Close() {
	slc.closeOnce.Do(func() {
		close(slc.closeCh)
		slc.readyConsumer.Close()
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

func (slc *singleLeveledConsumer) internalSubscribe(cli *client, options *singleLeveledConsumerOptions, lvl internal.TopicLevel, status internal.MessageStatus) (pulsar.Consumer, string, error) {
	groundTopic := options.Topics[0]
	subscriptionSuffix := "-" + options.SubscriptionName
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
		SubscriptionName:            options.SubscriptionName,
		Type:                        options.Type,
		SubscriptionInitialPosition: options.SubscriptionInitialPosition,
		NackRedeliveryDelay:         time.Second * time.Duration(options.NackRedeliveryDelay),
		NackBackoffPolicy:           options.NackBackoffDelayPolicy,
		MessageChannel:              nil,
	}
	if options.DLQ != nil {
		consumerOption.DLQ = &pulsar.DLQPolicy{
			MaxDeliveries:    options.DLQ.MaxDeliveries,
			RetryLetterTopic: options.DLQ.RetryLetterTopic,
			DeadLetterTopic:  options.DLQ.DeadLetterTopic,
		}
	}
	// only main level & ready status need compatible with pulsar retry enable and multi-topics
	if status == message.StatusReady && lvl == options.MainLevel {
		consumerOption.Topics = options.Topics
		consumerOption.RetryEnable = options.RetryEnable
	} else {
		consumerOption.Topics = nil
		consumerOption.RetryEnable = false
	}
	pulsarConsumer, err := cli.Client.Subscribe(consumerOption)
	return pulsarConsumer, topic, err
}
