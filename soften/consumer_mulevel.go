package soften

import (
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/internal"
)

type multiLeveledConsumer struct {
	logger           log.Logger
	messageCh        chan consumerMessage                        // channel used to deliver message to application
	leveledStrategy  internal.BalanceStrategy                    // 消费策略
	leveledPolicies  map[internal.TopicLevel]*config.LevelPolicy // 级别消费策略
	leveledConsumers map[internal.TopicLevel]*singleLeveledConsumer
}

func newMultiLeveledConsumer(parentLogger log.Logger, client *client, conf *config.ConsumerConfig, messageCh chan consumerMessage, levelHandlers map[internal.TopicLevel]*leveledConsumeDeciders) (*multiLeveledConsumer, error) {
	mlc := &multiLeveledConsumer{
		logger:          parentLogger.SubLogger(log.Fields{"levels": internal.TopicLevelParser.FormatList(conf.Levels)}),
		leveledStrategy: conf.LevelBalanceStrategy,
		leveledPolicies: conf.LevelPolicies,
		messageCh:       messageCh,
	}
	mlc.leveledConsumers = make(map[internal.TopicLevel]*singleLeveledConsumer, len(conf.Levels))
	for _, level := range conf.Levels {
		levelConsumer, err := newSingleLeveledConsumer(mlc.logger, client, level, conf, make(chan consumerMessage, 10), levelHandlers[level])
		if err != nil {
			return nil, fmt.Errorf("failed to new multi-status comsumer -> %v", err)
		}
		mlc.leveledConsumers[level] = levelConsumer
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
		msg, ok := messageChSelector.receiveOneByWeight(chs, balanceStrategy, &[]int{})
		if !ok {
			c.logger.Warnf("status chan closed")
			break
		}
		// 获取到消息
		if msg.Message != nil && msg.Consumer != nil {
			//fmt.Printf("received message  msgId: %v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
			c.messageCh <- msg
		} else {
			panic(fmt.Sprintf("consumed an invalid message: %v", msg))
		}
	}
}
