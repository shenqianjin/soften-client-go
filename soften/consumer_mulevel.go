package soften

import (
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/internal/concurrencylimit"
	"go.uber.org/ratelimit"
)

type multiLeveledConsumer struct {
	logger             log.Logger
	rateLimiter        ratelimit.Limiter
	concurrencyLimiter concurrencylimit.Limiter
	messageCh          chan consumerMessage                        // channel used to deliver message to application
	leveledStrategy    internal.BalanceStrategy                    // 消费策略
	leveledPolicies    map[internal.TopicLevel]*config.LevelPolicy // 级别消费策略
	leveledConsumers   map[internal.TopicLevel]*singleLeveledConsumer
}

func newMultiLeveledConsumer(parentLogger log.Logger, client *client, conf *config.ConsumerConfig, messageCh chan consumerMessage, levelHandlers map[internal.TopicLevel]*leveledConsumeDeciders) (*multiLeveledConsumer, error) {
	mlc := &multiLeveledConsumer{
		logger:          parentLogger.SubLogger(log.Fields{"levels": internal.TopicLevelParser.FormatList(conf.Levels)}),
		leveledStrategy: conf.LevelBalanceStrategy,
		leveledPolicies: conf.LevelPolicies,
		messageCh:       messageCh,
	}

	options := &singleLeveledConsumerOptions{
		Topics:                      conf.Topics,
		SubscriptionName:            conf.SubscriptionName,
		Type:                        conf.Type,
		SubscriptionInitialPosition: conf.SubscriptionInitialPosition,
		NackBackoffDelayPolicy:      conf.NackBackoffDelayPolicy,
		NackRedeliveryDelay:         conf.NackRedeliveryDelay,
		RetryEnable:                 conf.RetryEnable,
		DLQ:                         conf.DLQ,
		BalanceStrategy:             conf.StatusBalanceStrategy,
		MainLevel:                   conf.Levels[0],
		policy:                      conf.LevelPolicy,
	}
	mlc.leveledConsumers = make(map[internal.TopicLevel]*singleLeveledConsumer, len(conf.Levels))
	for _, level := range conf.Levels {
		options.policy = conf.LevelPolicies[level]
		levelConsumer, err := newSingleLeveledConsumer(mlc.logger, client, level, options, make(chan consumerMessage, 10), levelHandlers[level])
		if err != nil {
			return nil, fmt.Errorf("failed to new multi-status comsumer -> %v", err)
		}
		mlc.leveledConsumers[level] = levelConsumer
	}
	if *conf.ConsumerLimit.MaxOPS > 0 {
		mlc.rateLimiter = ratelimit.New(int(*conf.ConsumerLimit.MaxOPS))
	}
	if *conf.ConsumerLimit.MaxConcurrency > 0 {
		mlc.concurrencyLimiter = concurrencylimit.New(int(*conf.ConsumerLimit.MaxConcurrency))
	}
	// start to listen message from all status singleLeveledConsumer
	go mlc.retrieveLeveledMessages()
	return mlc, nil
}

func (c *multiLeveledConsumer) retrieveLeveledMessages() {
	chs := make([]<-chan consumerMessage, len(c.leveledConsumers))
	weights := make([]uint, len(c.leveledConsumers))
	for level, consumer := range c.leveledConsumers {
		chs = append(chs, consumer.Chan())
		weights = append(weights, c.leveledPolicies[level].ConsumeWeight)
	}
	balanceStrategy, err := config.BuildStrategy(c.leveledStrategy, weights)
	if err != nil {
		panic(fmt.Errorf("failed to start retrieve: %v", err))
	}
	for {
		// retrieve message from multiple levels
		msg, ok := messageChSelector.receiveOneByWeight(chs, balanceStrategy, &[]int{})
		if !ok {
			c.logger.Warnf("status chan closed")
			break
		}
		// 获取到消息
		// check rate limit
		if c.rateLimiter != nil {
			c.rateLimiter.Take()
		}
		// check concurrency limit
		if c.concurrencyLimiter != nil {
			c.concurrencyLimiter.Acquire()
			if originalDeferFunc := msg.internalExtra.deferFunc; originalDeferFunc != nil {
				msg.internalExtra.deferFunc = func() { originalDeferFunc(); c.concurrencyLimiter.Release() }
			} else {
				msg.internalExtra.deferFunc = func() { c.concurrencyLimiter.Release() }
			}
		}
		// delivery
		c.messageCh <- msg
	}
}
