package soften

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/shenqianjin/soften-client-go/soften/checker"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/internal"
)

type Producer interface {
	Send(ctx context.Context, msg *pulsar.ProducerMessage) (pulsar.MessageID, error)
	SendAsync(ctx context.Context, msg *pulsar.ProducerMessage, callback func(pulsar.MessageID, *pulsar.ProducerMessage, error))
	Close()
}

type producer struct {
	pulsar.Producer
	client         *client
	logger         log.Logger
	topic          string
	upgradeLevel   internal.TopicLevel
	degradeLevel   internal.TopicLevel
	enables        *internal.StatusEnables
	routePolicy    *config.RoutePolicy
	checkers       map[checker.CheckType]*wrappedProduceCheckpoint
	deciders       produceDeciders
	metrics        *internal.ProducerMetrics
	deciderMetrics sync.Map // map[internal.HandleGoto]*internal.DecideGotoMetrics
}

func newProducer(client *client, conf *config.ProducerConfig, checkpoints map[checker.CheckType]*checker.ProduceCheckpoint) (*producer, error) {
	options := pulsar.ProducerOptions{
		Topic: conf.Topic,
	}
	pulsarProducer, err := client.Client.CreateProducer(options)
	if err != nil {
		return nil, err
	}
	p := &producer{
		Producer:     pulsarProducer,
		client:       client,
		logger:       client.logger.SubLogger(log.Fields{"topic": options.Topic}),
		topic:        conf.Topic,
		upgradeLevel: conf.UpgradeTopicLevel,
		degradeLevel: conf.DegradeTopicLevel,
		routePolicy:  conf.Route,
		metrics:      client.metricsProvider.GetProducerMetrics(conf.Topic),
	}
	// collect enables
	p.enables = p.collectEnables(conf)
	// initialize checkers
	p.checkers = p.collectCheckers(p.enables, checkpoints)
	// initialize deciders
	produceDecidersOpts := p.formatDecidersOptions(conf)
	if deciders, err := newProduceDeciders(p, produceDecidersOpts); err != nil {
		return nil, err
	} else {
		p.deciders = deciders
	}
	p.logger.Infof("created soften producer")
	p.metrics.ProducersOpened.Inc()
	return p, nil
}

func (p *producer) collectEnables(conf *config.ProducerConfig) *internal.StatusEnables {
	enables := internal.StatusEnables{
		ReadyEnable:    true,
		DeadEnable:     conf.DeadEnable,
		DiscardEnable:  conf.DiscardEnable,
		BlockingEnable: conf.BlockingEnable,
		PendingEnable:  conf.PendingEnable,
		RetryingEnable: conf.RetryingEnable,
		RerouteEnable:  conf.RouteEnable,
		UpgradeEnable:  conf.UpgradeEnable,
		DegradeEnable:  conf.DegradeEnable,
	}
	return &enables
}

func (p *producer) collectCheckers(enables *internal.StatusEnables, checkpointMap map[checker.CheckType]*checker.ProduceCheckpoint) map[checker.CheckType]*wrappedProduceCheckpoint {
	checkers := make(map[checker.CheckType]*wrappedProduceCheckpoint)
	if enables.RerouteEnable {
		p.tryLoadConfiguredChecker(&checkers, checker.ProduceCheckTypeRoute, checkpointMap)
	}
	if enables.PendingEnable {
		p.tryLoadConfiguredChecker(&checkers, checker.ProduceCheckTypePending, checkpointMap)
	}
	if enables.BlockingEnable {
		p.tryLoadConfiguredChecker(&checkers, checker.ProduceCheckTypeBlocking, checkpointMap)
	}
	if enables.RetryingEnable {
		p.tryLoadConfiguredChecker(&checkers, checker.ProduceCheckTypeRetrying, checkpointMap)
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
	return checkers
}

func (p *producer) tryLoadConfiguredChecker(checkers *map[checker.CheckType]*wrappedProduceCheckpoint, checkType checker.CheckType, checkpointMap map[checker.CheckType]*checker.ProduceCheckpoint) {
	if ckp, ok := checkpointMap[checkType]; ok {
		metrics := p.client.metricsProvider.GetProducerTypedCheckMetrics(p.topic, checkType)
		(*checkers)[checkType] = newWrappedProduceCheckpoint(ckp, metrics)
	}
}

func (p *producer) formatDecidersOptions(conf *config.ProducerConfig) produceDecidersOptions {
	options := produceDecidersOptions{
		DiscardEnable:  conf.DiscardEnable,
		DeadEnable:     conf.DeadEnable,
		BlockingEnable: conf.BlockingEnable,
		PendingEnable:  conf.PendingEnable,
		RetryingEnable: conf.RetryingEnable,
		UpgradeEnable:  conf.UpgradeEnable,
		DegradeEnable:  conf.DegradeEnable,
		RouteEnable:    conf.RouteEnable,
		Route:          conf.Route,
	}
	return options
}

// Send aim to send message synchronously
func (p *producer) Send(ctx context.Context, msg *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	// do checkers
	for _, checkType := range checker.DefaultPrevSendCheckOrders() {
		if checkpoint, ok := p.checkers[checkType]; ok && checkpoint.CheckFunc != nil {
			checkStatus := p.internalCheck(checkpoint, msg)
			if handledDeferFunc := checkStatus.GetHandledDefer(); handledDeferFunc != nil {
				defer handledDeferFunc()
			}
			if !checkStatus.IsPassed() {
				continue
			}
			if mid, err, decided := p.internalSendDecideByCheckType(ctx, msg, checkType, checkStatus); decided {
				// return to skip biz decider if check handle succeeded
				return mid, err
			}
		}
	}
	// send
	start := time.Now()
	msgId, err := p.Producer.Send(ctx, msg)
	p.metrics.PublishLatency.Observe(time.Now().Sub(start).Seconds())
	return msgId, err
}

// SendAsync send message asynchronously
func (p *producer) SendAsync(ctx context.Context, msg *pulsar.ProducerMessage,
	callback func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
	start := time.Now()
	callbackNew := func(msgID pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
		p.metrics.PublishLatency.Observe(time.Now().Sub(start).Seconds())
		callback(msgID, msg, err)
	}
	// do checkers
	for _, checkType := range checker.DefaultPrevSendCheckOrders() {
		if checkpoint, ok := p.checkers[checkType]; ok && checkpoint.CheckFunc != nil {
			checkStatus := p.internalCheck(checkpoint, msg)
			if handledDeferFunc := checkStatus.GetHandledDefer(); handledDeferFunc != nil {
				defer handledDeferFunc()
			}
			if !checkStatus.IsPassed() {
				continue
			}
			if _, decided := p.internalSendAsyncDecideByCheckType(ctx, msg, checkType, checkStatus, callback); decided {
				// return to skip biz decider if check handle succeeded
				return
			}
		}
	}
	// send async
	p.Producer.SendAsync(ctx, msg, callbackNew)
	return
}

func (p *producer) internalCheck(checkpoint *wrappedProduceCheckpoint, msg *pulsar.ProducerMessage) checker.CheckStatus {
	start := time.Now()
	checkStatus := checkpoint.CheckFunc(msg)
	latency := time.Now().Sub(start).Seconds()
	checkpoint.metrics.CheckLatency.Observe(latency)
	if checkStatus.IsPassed() {
		checkpoint.metrics.CheckPassed.Inc()
	} else {
		checkpoint.metrics.CheckRejected.Inc()
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
	metrics := p.getDecideMetrics(msgGoto)

	start := time.Now()
	mid, err, decided = decider.Decide(ctx, msg, checkStatus)
	latency := time.Since(start).Seconds()
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
	metrics := p.getDecideMetrics(msgGoto)

	start := time.Now()
	decided = decider.DecideAsync(ctx, msg, checkStatus, callback)
	latency := time.Since(start).Seconds()
	metrics.DecideLatency.Observe(latency)
	if decided {
		metrics.DecideSuccess.Inc()
	} else {
		metrics.DecideFailed.Inc()
	}
	return err, decided
}

func (p *producer) internalGetDeciderByGoto(msgGoto internal.HandleGoto) internalProduceDecider {
	if decider, ok := p.deciders[msgGoto]; ok {
		return decider
	} else {
		p.logger.Warnf("invalid msg goto action: %v", msgGoto)
		return nil
	}
}

func (p *producer) getDecideMetrics(msgGoto internal.HandleGoto) *internal.DecideGotoMetrics {
	if metrics, ok := p.deciderMetrics.Load(msgGoto); ok {
		return metrics.(*internal.DecideGotoMetrics)
	}
	metrics := p.client.metricsProvider.GetProducerDecideGotoMetrics(p.topic, msgGoto.String())
	p.deciderMetrics.Store(msgGoto, metrics)
	return metrics
}

func (p *producer) Close() {
	p.Producer.Close()
	for _, decider := range p.deciders {
		decider.close()
	}
	p.logger.Info("closed soften producer")
	p.metrics.ProducersOpened.Dec()
}

// ------ helper ------

type wrappedProduceCheckpoint struct {
	*checker.ProduceCheckpoint
	metrics *internal.TypedCheckMetrics
}

func newWrappedProduceCheckpoint(ckp *checker.ProduceCheckpoint, metrics *internal.TypedCheckMetrics) *wrappedProduceCheckpoint {
	wrappedCkp := &wrappedProduceCheckpoint{ProduceCheckpoint: ckp, metrics: metrics}
	wrappedCkp.metrics.CheckersOpened.Inc()
	return wrappedCkp
}

func (c *wrappedProduceCheckpoint) Close() {
	c.metrics.CheckersOpened.Dec()
}
