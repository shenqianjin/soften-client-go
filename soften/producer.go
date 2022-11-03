package soften

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/pkg/errors"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/shenqianjin/soften-client-go/soften/support/meta"
)

type Producer interface {
	// Send sends a message
	Send(ctx context.Context, msg *pulsar.ProducerMessage) (pulsar.MessageID, error)

	// SendAsync sends a message in asynchronous mode
	SendAsync(ctx context.Context, msg *pulsar.ProducerMessage, callback func(pulsar.MessageID, *pulsar.ProducerMessage, error))

	// Close the producer and releases resources allocated
	Close()
}

type producer struct {
	pulsar.Producer
	client          *client
	logger          log.Logger
	groundTopic     string // same with topic for producer
	level           internal.TopicLevel
	topic           string //
	upgradePolicy   *config.ShiftPolicy
	degradePolicy   *config.ShiftPolicy
	enables         *internal.ProduceEnables
	routePolicy     *config.TransferPolicy
	prevCheckOrders []checker.CheckType
	checkers        map[checker.CheckType]*produceCheckpointChain
	deciders        produceDeciders
	metricsProvider *internal.MetricsProvider
	backoff         *config.BackoffPolicy // 发送失败补偿次数。默认30次; 前7次60s,累计24分钟
}

func newProducer(client *client, conf *config.ProducerConfig, checkpoints map[checker.CheckType][]*checker.ProduceCheckpoint) (*producer, error) {
	options := pulsar.ProducerOptions{
		Topic:                   conf.Topic + conf.Level.TopicSuffix(),
		Name:                    conf.Name,
		SendTimeout:             time.Second * time.Duration(conf.SendTimeout),
		MaxPendingMessages:      conf.MaxPendingMessages,
		HashingScheme:           pulsar.HashingScheme(conf.HashingScheme),
		CompressionType:         pulsar.CompressionType(conf.CompressionType),
		CompressionLevel:        pulsar.CompressionLevel(conf.CompressionLevel),
		DisableBatching:         conf.DisableBatching,
		BatchingMaxPublishDelay: time.Second * time.Duration(conf.BatchingMaxPublishDelay),
		BatchingMaxMessages:     conf.BatchingMaxMessages,
		BatchingMaxSize:         conf.BatchingMaxSize,
		BatcherBuilderType:      pulsar.BatcherBuilderType(conf.BatcherBuilderType),
	}
	pulsarProducer, err := client.Client.CreateProducer(options)
	if err != nil {
		return nil, err
	}
	p := &producer{
		Producer:        pulsarProducer,
		client:          client,
		logger:          client.logger.SubLogger(log.Fields{"ground_topic": conf.Topic, "level": conf.Level}),
		groundTopic:     conf.Topic,
		topic:           conf.Topic + conf.Level.TopicSuffix(),
		level:           conf.Level,
		upgradePolicy:   conf.Upgrade,
		degradePolicy:   conf.Degrade,
		routePolicy:     conf.Transfer,
		metricsProvider: client.metricsProvider,
		backoff:         conf.Backoff,
	}
	// collect enables
	p.enables = p.collectEnables(conf)
	// initialize checkers
	p.checkers = p.collectCheckers(p.enables, checkpoints)
	// collect check orders
	p.prevCheckOrders = p.collectCheckOrders()
	// initialize deciders
	produceDecidersOpts := p.formatDecidersOptions(conf)
	if deciders, err := newProduceDeciders(p, produceDecidersOpts); err != nil {
		return nil, err
	} else {
		p.deciders = deciders
	}
	p.logger.Infof("created soften producer")
	p.metricsProvider.GetProducerMetrics(p.groundTopic, p.topic).ProducersOpened.Inc()
	return p, nil
}

func (p *producer) collectEnables(conf *config.ProducerConfig) *internal.ProduceEnables {
	enables := internal.ProduceEnables{
		DiscardEnable:  *conf.DiscardEnable,
		DeadEnable:     *conf.DeadEnable,
		TransferEnable: *conf.TransferEnable,
		UpgradeEnable:  *conf.UpgradeEnable,
		DegradeEnable:  *conf.DegradeEnable,
		ShiftEnable:    *conf.ShiftEnable,
	}
	return &enables
}

func (p *producer) collectCheckers(enables *internal.ProduceEnables, checkpointMap map[checker.CheckType][]*checker.ProduceCheckpoint) map[checker.CheckType]*produceCheckpointChain {
	checkers := make(map[checker.CheckType]*produceCheckpointChain)
	if enables.TransferEnable {
		p.tryLoadConfiguredChecker(&checkers, checker.ProduceCheckTypeTransfer, checkpointMap)
	}
	if enables.DeadEnable {
		p.tryLoadConfiguredChecker(&checkers, checker.ProduceCheckTypeDead, checkpointMap)
	}
	if enables.DiscardEnable {
		p.tryLoadConfiguredChecker(&checkers, checker.ProduceCheckTypeDiscard, checkpointMap)
	}
	if enables.UpgradeEnable {
		p.tryLoadConfiguredChecker(&checkers, checker.ProduceCheckTypeUpgrade, checkpointMap)
	}
	if enables.DegradeEnable {
		p.tryLoadConfiguredChecker(&checkers, checker.ProduceCheckTypeDegrade, checkpointMap)
	}
	if enables.ShiftEnable {
		p.tryLoadConfiguredChecker(&checkers, checker.ProduceCheckTypeShift, checkpointMap)
	}
	return checkers
}

func (p *producer) collectCheckOrders() []checker.CheckType {
	prevCheckOrders := make([]checker.CheckType, 0)
	for _, checkType := range checker.DefaultPrevSendCheckOrders() {
		if _, ok := p.checkers[checkType]; ok {
			prevCheckOrders = append(prevCheckOrders, checkType)
		}
	}
	return prevCheckOrders
}

func (p *producer) tryLoadConfiguredChecker(checkers *map[checker.CheckType]*produceCheckpointChain, checkType checker.CheckType, checkpointMap map[checker.CheckType][]*checker.ProduceCheckpoint) {
	if checkChains, ok := checkpointMap[checkType]; ok {
		options := produceCheckpointChainOptions{groundTopic: p.groundTopic, topic: p.topic, checkType: checkType}
		(*checkers)[checkType] = newProduceCheckpointChains(checkChains, options, p.metricsProvider)
	}
}

func (p *producer) formatDecidersOptions(conf *config.ProducerConfig) produceDecidersOptions {
	options := produceDecidersOptions{
		groundTopic:    conf.Topic,
		level:          conf.Level,
		DiscardEnable:  *conf.DiscardEnable,
		DeadEnable:     *conf.DeadEnable,
		Dead:           conf.Dead,
		UpgradeEnable:  *conf.UpgradeEnable,
		Upgrade:        conf.Upgrade,
		DegradeEnable:  *conf.DegradeEnable,
		Degrade:        conf.Degrade,
		ShiftEnable:    *conf.ShiftEnable,
		Shift:          conf.Shift,
		TransferEnable: *conf.TransferEnable,
		Transfer:       conf.Transfer,
	}
	return options
}

// Send sends a message
func (p *producer) Send(ctx context.Context, msg *pulsar.ProducerMessage) (msgId pulsar.MessageID, err error) {
	start := time.Now()
	ctx = meta.AsMetas(ctx)
	// init map if necessary
	if msg.Properties == nil {
		msg.Properties = make(map[string]string)
	}
	// do checkers
	cMsg := &message.ProducerMessage{ProducerMessage: msg, LeveledMessage: &leveledMessage{level: p.level}}
	for _, checkType := range p.prevCheckOrders {
		if wpCheckpoint, ok := p.checkers[checkType]; ok && len(wpCheckpoint.checkChains) > 0 {
			var chkStatus checker.CheckStatus
			for ci := 0; ci < len(wpCheckpoint.checkChains); ci++ {
				chkStatus = p.internalCheck(ctx, wpCheckpoint, ci, cMsg)
				if handledDeferFunc := chkStatus.GetHandledDefer(); handledDeferFunc != nil {
					defer handledDeferFunc()
				}
				if chkStatus != nil && chkStatus.IsPassed() {
					break
				}
			}
			if chkStatus == nil || !chkStatus.IsPassed() {
				continue
			}
			if mid, err, decided := p.internalSendDecideByCheckType(ctx, msg, checkType, chkStatus); decided {
				// return to skip biz decider if check handle succeeded
				return mid, err
			}
		}
	}
	// inject message counter
	initMsgCounter := internal.MessageCounter{PublishTimes: 1}
	message.Helper.InjectMessageCounter(msg.Properties, initMsgCounter)
	message.Helper.InjectStatusMessageCounter(msg.Properties, message.StatusReady, initMsgCounter)
	// send
	msgId, err = p.Producer.Send(ctx, msg)
	// backoff
	for sendTimes := uint(1); err != nil && *p.backoff.MaxTimes > 0 && sendTimes <= *p.backoff.MaxTimes; sendTimes++ {
		delay := p.backoff.DelayPolicy.Next(int(sendTimes))
		time.Sleep(time.Duration(delay) * time.Second)
		msgId, err = p.Producer.Send(ctx, msg)
	}
	if err == nil {
		now := time.Now()
		p.metricsProvider.GetProducerMetrics(p.groundTopic, p.topic).PublishLatencyFromPCheck.Observe(now.Sub(start).Seconds())
		if !msg.EventTime.IsZero() && msg.EventTime.After(internal.EarliestEventTime) {
			p.metricsProvider.GetProducerMetrics(p.groundTopic, p.topic).PublishLatencyFromEvent.Observe(now.Sub(msg.EventTime).Seconds())
		}
		p.metricsProvider.GetProducerMetrics(p.groundTopic, p.topic).PublishSuccess.Inc()
	} else {
		p.metricsProvider.GetProducerMetrics(p.groundTopic, p.topic).PublishFailed.Inc()
	}
	return msgId, err
}

// SendAsync sends a message in asynchronous
func (p *producer) SendAsync(ctx context.Context, msg *pulsar.ProducerMessage,
	callback func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
	ctx = meta.AsMetas(ctx)
	start := time.Now()
	// init map if necessary
	if msg.Properties == nil {
		msg.Properties = make(map[string]string)
	}
	callbackNew := func(msgID pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
		// backoff with synchronous send
		for sendTimes := uint(1); err != nil && *p.backoff.MaxTimes > 0 && sendTimes <= *p.backoff.MaxTimes; sendTimes++ {
			delay := p.backoff.DelayPolicy.Next(int(sendTimes))
			time.Sleep(time.Duration(delay) * time.Second)
			msgID, err = p.Producer.Send(ctx, msg)
		}
		if err == nil {
			now := time.Now()
			p.metricsProvider.GetProducerMetrics(p.groundTopic, p.topic).PublishLatencyFromPCheck.Observe(now.Sub(start).Seconds())
			if !msg.EventTime.IsZero() && msg.EventTime.After(internal.EarliestEventTime) {
				p.metricsProvider.GetProducerMetrics(p.groundTopic, p.topic).PublishLatencyFromEvent.Observe(now.Sub(msg.EventTime).Seconds())
			}
			p.metricsProvider.GetProducerMetrics(p.groundTopic, p.topic).PublishSuccess.Inc()
		} else {
			p.metricsProvider.GetProducerMetrics(p.groundTopic, p.topic).PublishFailed.Inc()
		}
		callback(msgID, msg, err)
	}
	// do checkers
	cMsg := &message.ProducerMessage{ProducerMessage: msg, LeveledMessage: &leveledMessage{level: p.level}}
	for _, checkType := range p.prevCheckOrders {
		if wpCheckpoint, ok := p.checkers[checkType]; ok && len(wpCheckpoint.checkChains) > 0 {
			var chkStatus checker.CheckStatus
			for ci := 0; ci < len(wpCheckpoint.checkChains); ci++ {
				chkStatus = p.internalCheck(ctx, wpCheckpoint, ci, cMsg)
				if handledDeferFunc := chkStatus.GetHandledDefer(); handledDeferFunc != nil {
					defer handledDeferFunc()
				}
				if chkStatus != nil && chkStatus.IsPassed() {
					break
				}
			}
			if chkStatus == nil || !chkStatus.IsPassed() {
				continue
			}
			if _, decided := p.internalSendAsyncDecideByCheckType(ctx, msg, checkType, chkStatus, callbackNew); decided {
				// return to skip biz decider if check handle succeeded
				return
			}
		}
	}
	// inject message counter
	initMsgCounter := internal.MessageCounter{PublishTimes: 1}
	message.Helper.InjectMessageCounter(msg.Properties, initMsgCounter)
	message.Helper.InjectStatusMessageCounter(msg.Properties, message.StatusReady, initMsgCounter)
	// send async
	p.Producer.SendAsync(ctx, msg, callbackNew)
	return
}

func (p *producer) internalCheck(ctx context.Context, chain *produceCheckpointChain, ci int, msg *message.ProducerMessage) checker.CheckStatus {
	// execute check chains
	start := time.Now()
	checkpoint := chain.checkChains[ci]
	checkStatus := checkpoint.CheckFunc(ctx, msg)

	// observe metrics
	latency := time.Now().Sub(start).Seconds()
	metrics := p.metricsProvider.GetProducerCheckerMetrics(chain.options.groundTopic, chain.options.topic, chain.options.checkType.String())
	metrics.CheckLatency.Observe(latency)
	if checkStatus.IsPassed() {
		metrics.CheckPassed.Inc()
	} else {
		metrics.CheckRejected.Inc()
	}
	return checkStatus
}

func (p *producer) internalSendDecideByCheckType(ctx context.Context, msg *pulsar.ProducerMessage, checkType checker.CheckType, checkStatus checker.CheckStatus) (mid pulsar.MessageID, err error, decided bool) {
	msgGoto, ok := checkTypeGotoMap[checkType]
	if !ok {
		return nil, errors.New(fmt.Sprintf("%s is not supported for any decider", checkType)), false
	}
	decider := p.internalGetDeciderByGoto(msgGoto)
	if decider == nil {
		p.logger.Warnf("failed to get decider by checkType: %v, err: %v", checkType, err)
		return nil, err, false
	}

	start := time.Now()
	mid, err, decided = decider.Decide(ctx, msg, checkStatus)
	latency := time.Since(start).Seconds()
	metrics := p.metricsProvider.GetProducerDeciderMetrics(p.groundTopic, p.topic, msgGoto.String())
	metrics.DecideLatency.Observe(latency)
	if decided {
		metrics.DecideSuccess.Inc()
	} else {
		metrics.DecideFailed.Inc()
	}
	return mid, err, decided
}

func (p *producer) internalSendAsyncDecideByCheckType(ctx context.Context, msg *pulsar.ProducerMessage,
	checkType checker.CheckType, checkStatus checker.CheckStatus,
	callback func(pulsar.MessageID, *pulsar.ProducerMessage, error)) (err error, decided bool) {
	msgGoto, ok := checkTypeGotoMap[checkType]
	if !ok {
		return errors.New(fmt.Sprintf("%s is not supported for any decider", checkType)), false
	}
	decider := p.internalGetDeciderByGoto(msgGoto)
	if decider == nil {
		p.logger.Warnf("failed to get decider by checkType: %v, err: %v", checkType, err)
		return err, false
	}

	start := time.Now()
	decided = decider.DecideAsync(ctx, msg, checkStatus, callback)
	latency := time.Since(start).Seconds()
	metrics := p.metricsProvider.GetProducerDeciderMetrics(p.groundTopic, p.topic, msgGoto.String())
	metrics.DecideLatency.Observe(latency)
	if decided {
		metrics.DecideSuccess.Inc()
	} else {
		metrics.DecideFailed.Inc()
	}
	return err, decided
}

func (p *producer) internalGetDeciderByGoto(msgGoto internal.DecideGoto) internalProduceDecider {
	if decider, ok := p.deciders[msgGoto]; ok {
		return decider
	} else {
		p.logger.Warnf("invalid msg goto action: %v", msgGoto)
		return nil
	}
}

func (p *producer) Close() {
	p.Producer.Close()
	for _, decider := range p.deciders {
		decider.close()
	}
	p.logger.Info("closed soften producer")
	p.metricsProvider.GetProducerMetrics(p.groundTopic, p.topic).ProducersOpened.Dec()
}

// ------ helper ------

type produceCheckpointChain struct {
	checkChains     []*checker.ProduceCheckpoint
	options         produceCheckpointChainOptions
	metricsProvider *internal.MetricsProvider
}

type produceCheckpointChainOptions struct {
	groundTopic string
	topic       string
	checkType   checker.CheckType
}

func newProduceCheckpointChains(checkChains []*checker.ProduceCheckpoint, options produceCheckpointChainOptions,
	metricsProvider *internal.MetricsProvider) *produceCheckpointChain {
	c := &produceCheckpointChain{checkChains: checkChains, metricsProvider: metricsProvider, options: options}
	c.metricsProvider.GetProducerCheckersMetrics(c.options.groundTopic, c.options.topic, c.options.checkType.String()).CheckersOpened.Inc()
	return c
}

func (c *produceCheckpointChain) Close() {
	c.metricsProvider.GetProducerCheckersMetrics(c.options.groundTopic, c.options.topic, c.options.checkType.String()).CheckersOpened.Dec()

}
