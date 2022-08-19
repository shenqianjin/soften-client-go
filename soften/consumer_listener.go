package soften

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/panjf2000/ants/v2"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/handler"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

type Listener interface {
	Start(ctx context.Context, handler handler.HandleFunc) error
	StartPremium(ctx context.Context, handler handler.PremiumHandleFunc) error
	Close()
}

// consumeListener listens to consume all messages of one or more than one status/levels consumers.
type consumeListener struct {
	client               *client
	logger               log.Logger
	messageCh            chan ConsumerMessage // channel used to deliver message to application
	enables              *internal.StatusEnables
	concurrency          *config.ConcurrencyPolicy
	generalDeciders      *generalConsumeDeciders
	levelDeciders        map[internal.TopicLevel]*leveledConsumeDeciders
	checkers             map[checker.CheckType]*wrappedCheckpoint
	prevCheckOrders      []checker.CheckType
	postCheckOrders      []checker.CheckType
	startListenerOnce    sync.Once
	closeListenerOnce    sync.Once
	multiLeveledConsumer *multiLeveledConsumer
	leveledConsumer      *leveledConsumer
	logTopics            string
	logLevels            string
	metrics              *internal.ListenMetrics
	deciderMetrics       sync.Map // map[internal.HandleGoto]*internal.ConsumerHandleGotoMetrics
}

func newConsumeListener(cli *client, conf config.ConsumerConfig, checkpoints map[checker.CheckType]*checker.ConsumeCheckpoint) (*consumeListener, error) {
	logTopic := conf.Topics[0]
	if len(conf.Topics) > 1 {
		logTopic = logTopic + "+" + strconv.Itoa(len(conf.Topics)-1)
	}
	logLevels := internal.TopicLevelParser.FormatList(conf.Levels)
	topicLogger := cli.logger.SubLogger(log.Fields{"Topic": logTopic})
	listener := &consumeListener{
		client:      cli,
		messageCh:   make(chan ConsumerMessage, 10),
		logger:      topicLogger.SubLogger(log.Fields{"level": logLevels}),
		concurrency: conf.Concurrency,
		metrics:     cli.metricsProvider.GetListenMetrics(logTopic, logLevels),
		logTopics:   logTopic,
		logLevels:   logLevels,
	}
	// collect enables
	listener.enables = listener.collectEnables(&conf)
	// initialize checkers
	listener.checkers = listener.collectCheckers(listener.enables, checkpoints)
	// collect check orders
	listener.prevCheckOrders, listener.postCheckOrders = listener.collectCheckOrders()
	// initialize general deciders
	generalHdOptions := listener.formatGeneralDecidersOptions(conf.Topics[0], &conf)
	if deciders, err := newGeneralConsumeDeciders(cli, listener, generalHdOptions); err != nil {
		return nil, err
	} else {
		listener.generalDeciders = deciders
	}
	// initialize level related deciders
	listener.levelDeciders = make(map[internal.TopicLevel]*leveledConsumeDeciders, len(conf.Levels))
	for _, level := range conf.Levels {
		suffix := level.TopicSuffix()
		options := listener.formatLeveledDecidersOptions(conf.Topics[0]+suffix, level, &conf)
		if deciders, err := newLeveledConsumeDeciders(cli, listener, options, listener.generalDeciders.deadDecider); err != nil {
			return nil, err
		} else {
			listener.levelDeciders[level] = deciders
		}
	}
	// initialize status leveledConsumer
	if len(conf.Levels) == 1 {
		level := conf.Levels[0]
		if _, err := newSingleLeveledConsumer(topicLogger, cli, level, &conf, listener.messageCh, listener.levelDeciders[level]); err != nil {
			return nil, err
		}
	} else {
		if _, err := newMultiLeveledConsumer(topicLogger, cli, &conf, listener.messageCh, listener.levelDeciders); err != nil {
			return nil, err
		}
	}

	listener.logger.Infof("created consume listener. topics: %v", conf.Topics)
	listener.metrics.ListenersOpened.Inc()
	return listener, nil
}

func (l *consumeListener) collectEnables(conf *config.ConsumerConfig) *internal.StatusEnables {
	enables := internal.StatusEnables{
		ReadyEnable:    true,
		DeadEnable:     conf.DeadEnable,
		DiscardEnable:  conf.DiscardEnable,
		BlockingEnable: conf.BlockingEnable,
		PendingEnable:  conf.PendingEnable,
		RetryingEnable: conf.RetryingEnable,
		RerouteEnable:  conf.RerouteEnable,
		UpgradeEnable:  conf.UpgradeEnable,
		DegradeEnable:  conf.DegradeEnable,
	}
	return &enables
}

func (l *consumeListener) collectCheckers(enables *internal.StatusEnables, checkpointMap map[checker.CheckType]*checker.ConsumeCheckpoint) map[checker.CheckType]*wrappedCheckpoint {
	checkers := make(map[checker.CheckType]*wrappedCheckpoint)
	if enables.RerouteEnable {
		l.tryLoadConfiguredChecker(&checkers, checker.CheckTypePrevReroute, checkpointMap)
		l.tryLoadConfiguredChecker(&checkers, checker.CheckTypePostReroute, checkpointMap)
	}
	if enables.PendingEnable {
		l.tryLoadConfiguredChecker(&checkers, checker.CheckTypePrevPending, checkpointMap)
		l.tryLoadConfiguredChecker(&checkers, checker.CheckTypePostPending, checkpointMap)
	}
	if enables.BlockingEnable {
		l.tryLoadConfiguredChecker(&checkers, checker.CheckTypePrevBlocking, checkpointMap)
		l.tryLoadConfiguredChecker(&checkers, checker.CheckTypePostBlocking, checkpointMap)
	}
	if enables.RetryingEnable {
		l.tryLoadConfiguredChecker(&checkers, checker.CheckTypePrevRetrying, checkpointMap)
		l.tryLoadConfiguredChecker(&checkers, checker.CheckTypePostRetrying, checkpointMap)
	}
	if enables.DeadEnable {
		l.tryLoadConfiguredChecker(&checkers, checker.CheckTypePrevDead, checkpointMap)
		l.tryLoadConfiguredChecker(&checkers, checker.CheckTypePostDead, checkpointMap)
	}
	if enables.DiscardEnable {
		l.tryLoadConfiguredChecker(&checkers, checker.CheckTypePrevDiscard, checkpointMap)
		l.tryLoadConfiguredChecker(&checkers, checker.CheckTypePostDiscard, checkpointMap)
	}
	if enables.UpgradeEnable {
		l.tryLoadConfiguredChecker(&checkers, checker.CheckTypePrevUpgrade, checkpointMap)
		l.tryLoadConfiguredChecker(&checkers, checker.CheckTypePostUpgrade, checkpointMap)
	}
	if enables.DegradeEnable {
		l.tryLoadConfiguredChecker(&checkers, checker.CheckTypePrevDegrade, checkpointMap)
		l.tryLoadConfiguredChecker(&checkers, checker.CheckTypePostDegrade, checkpointMap)
	}
	return checkers
}

func (l *consumeListener) tryLoadConfiguredChecker(checkers *map[checker.CheckType]*wrappedCheckpoint, checkType checker.CheckType, checkpointMap map[checker.CheckType]*checker.ConsumeCheckpoint) {
	if ckp, ok := checkpointMap[checkType]; ok {
		metrics := l.client.metricsProvider.GetListenerTypedCheckMetrics(l.logTopics, l.logLevels, checkType)
		(*checkers)[checkType] = newWrappedCheckpoint(ckp, metrics)
	}
}

func (l *consumeListener) collectCheckOrders() ([]checker.CheckType, []checker.CheckType) {
	prevCheckOrders := make([]checker.CheckType, 0)
	for _, checkType := range checker.DefaultPrevHandleCheckOrders() {
		if _, ok := l.checkers[checkType]; ok {
			prevCheckOrders = append(prevCheckOrders, checkType)
		}
	}
	postCheckOrders := make([]checker.CheckType, 0)
	for _, checkType := range checker.DefaultPostHandleCheckOrders() {
		if _, ok := l.checkers[checkType]; ok {
			postCheckOrders = append(postCheckOrders, checkType)
		}
	}
	return prevCheckOrders, postCheckOrders
}

func (l *consumeListener) formatGeneralDecidersOptions(topic string, conf *config.ConsumerConfig) generalConsumeDeciderOptions {
	options := generalConsumeDeciderOptions{
		Topic:         topic,
		DiscardEnable: conf.DiscardEnable,
		DeadEnable:    conf.DeadEnable,
		RerouteEnable: conf.RerouteEnable,
		Reroute:       conf.Reroute,
	}
	return options
}

func (l *consumeListener) formatLeveledDecidersOptions(topic string, level internal.TopicLevel, config *config.ConsumerConfig) leveledConsumeDeciderOptions {
	options := leveledConsumeDeciderOptions{
		Topic:             topic,
		Level:             level,
		BlockingEnable:    config.BlockingEnable,
		Blocking:          config.Blocking,
		PendingEnable:     config.PendingEnable,
		Pending:           config.Pending,
		RetryingEnable:    config.RetryingEnable,
		Retrying:          config.Retrying,
		UpgradeEnable:     config.UpgradeEnable,
		UpgradeTopicLevel: config.UpgradeTopicLevel,
		DegradeEnable:     config.DegradeEnable,
		DegradeTopicLevel: config.DegradeTopicLevel,
	}
	return options
}

func (l *consumeListener) Start(ctx context.Context, handleFunc handler.HandleFunc) error {
	// convert decider
	premiumHandler := func(message pulsar.Message) handler.HandleStatus {
		success, err := handleFunc(message)
		if success {
			return handler.HandleStatusOk
		} else {
			return handler.HandleStatusBuilder().Err(err).Build()
		}
	}
	// forward the call to l.SubscribePremium
	return l.StartPremium(ctx, premiumHandler)
}

// StartPremium blocking to consume message one by one. it returns error if any parameters is invalid
func (l *consumeListener) StartPremium(ctx context.Context, handleFunc handler.PremiumHandleFunc) (err error) {
	// validate decider
	if handleFunc == nil {
		return errors.New("decider parameter is nil")
	}
	l.startListenerOnce.Do(func() {
		// initialize task pool
		pool, onceErr := ants.NewPool(int(l.concurrency.CorePoolSize), ants.WithExpiryDuration(60*time.Second))
		if onceErr != nil {
			err = onceErr
			return
		}
		// listen in async
		go func() {
			l.logger.Info("started to listening...")
			l.metrics.ListenersRunning.Inc()
			// receive msg and then consume one by one
			l.internalStartInPool(ctx, handleFunc, pool)
			l.metrics.ListenersRunning.Dec()
			l.logger.Info("ended to listening")
		}()
	})
	return nil
}

func (l *consumeListener) internalStartInPool(ctx context.Context, handler handler.PremiumHandleFunc, pool *ants.Pool) {
	// receive msg and submit task
	count := 0
	for {
		select {
		case msg, ok := <-l.messageCh:
			if !ok {
				return
			}
			count++
			// As pool.Submit is blocking, err happens only if pool is closed.
			// Namely, the 'err != nil' condition is never meet.
			if err := pool.Submit(func() { l.consume(handler, msg) }); err != nil {
				l.logger.Errorf("submit msg failed. err: %v", err)
				//msg.Consumer.Nack(msg.Message)
				//return
			}
			//l.logger.Infof("consume end -------+++++++++++++++++++++++++++- %d", count)
		case <-ctx.Done():
			l.logger.Warnf("closed soften listener")
			return
		}
	}
}

func (l *consumeListener) internalStartInParallel(ctx context.Context, handler handler.PremiumHandleFunc) {
	concurrencyChan := make(chan bool, l.concurrency.CorePoolSize)
	for {
		select {
		case msg, ok := <-l.messageCh:
			if !ok {
				return
			}
			concurrencyChan <- true
			go func(msg ConsumerMessage) {
				l.consume(handler, msg)
				<-concurrencyChan
			}(msg)
		case <-ctx.Done():
			return
		}

	}
}

func (l *consumeListener) consume(handler handler.PremiumHandleFunc, msg ConsumerMessage) {
	// prev-check to handle in turn
	for _, checkType := range l.prevCheckOrders {
		if checkpoint, ok := l.checkers[checkType]; ok && checkpoint.Prev != nil {
			checkStatus := l.internalPrevCheck(checkpoint, msg)
			if handledDeferFunc := checkStatus.GetHandledDefer(); handledDeferFunc != nil {
				defer handledDeferFunc()
			}
			if !checkStatus.IsPassed() {
				continue
			}
			if decided := l.internalDecideByPrevCheckType(msg, checkType, checkStatus); decided {
				// return to skip biz decider if check handle succeeded
				return
			}
		}
	}

	// do x handle
	start := time.Now()
	bizHandleStatus := handler(msg)
	latency := time.Now().Sub(start).Seconds()
	consumeTimes := message.Parser.GetXReconsumeTimes(msg.ConsumerMessage)
	handleMetrics := l.getHandleMetrics(msg, bizHandleStatus.GetGoto())
	handleMetrics.HandleGoto.Inc()
	handleMetrics.HandleGotoLatency.Observe(latency)
	handleMetrics.HandleGotoConsumeTimes.Observe(float64(consumeTimes))

	// post-check to route - for obvious goto action
	if bizHandleStatus.GetGoto() != "" {
		if decided := l.internalDecide4Goto(bizHandleStatus.GetGoto(), msg, checker.CheckStatusPassed, handleMetrics); decided {
			// return if handle succeeded
			return
		}
	}

	// post-check to route - for obvious checkers or configured checkers
	postCheckTypesInTurn := l.postCheckOrders
	if len(bizHandleStatus.GetCheckTypes()) > 0 {
		postCheckTypesInTurn = bizHandleStatus.GetCheckTypes()
	}
	for _, checkType := range postCheckTypesInTurn {
		if checkpoint, ok := l.checkers[checkType]; ok && checkpoint.Post != nil {
			checkStatus := l.internalPostCheck(checkpoint, msg, bizHandleStatus.GetErr())
			if handledDeferFunc := checkStatus.GetHandledDefer(); handledDeferFunc != nil {
				defer handledDeferFunc()
			}
			if !checkStatus.IsPassed() {
				continue
			}
			if decided := l.internalDecideByPostCheckType(msg, checkType, checkStatus, handleMetrics); decided {
				// return if check handle succeeded
				return
			}
		}
	}

	// here means to let application client Ack/Nack msg
	return
}

func (l *consumeListener) internalPrevCheck(checkpoint *wrappedCheckpoint, msg ConsumerMessage) checker.CheckStatus {
	start := time.Now()
	checkStatus := checkpoint.Prev(msg)
	latency := time.Now().Sub(start).Seconds()
	checkpoint.metrics.CheckLatency.Observe(latency)
	if checkStatus.IsPassed() {
		checkpoint.metrics.CheckPassed.Inc()
	} else {
		checkpoint.metrics.CheckRejected.Inc()
	}
	return checkStatus
}

func (l *consumeListener) internalPostCheck(checkpoint *wrappedCheckpoint, msg ConsumerMessage, err error) checker.CheckStatus {
	start := time.Now()
	checkStatus := checkpoint.Post(msg, err)
	latency := time.Now().Sub(start).Seconds()
	checkpoint.metrics.CheckLatency.Observe(latency)
	if checkStatus.IsPassed() {
		checkpoint.metrics.CheckPassed.Inc()
	} else {
		checkpoint.metrics.CheckRejected.Inc()
	}
	return checkStatus
}

func (l *consumeListener) internalDecideByPrevCheckType(msg ConsumerMessage, checkType checker.CheckType, checkStatus checker.CheckStatus) (ok bool) {
	msgGoto, ok := checkTypeGotoMap[checkType]
	if !ok {
		return false
	}
	metrics := l.getHandleMetrics(msg, msgGoto)

	return l.internalDecide4Goto(msgGoto, msg, checkStatus, metrics)
}

func (l *consumeListener) internalDecideByPostCheckType(msg ConsumerMessage, checkType checker.CheckType, checkStatus checker.CheckStatus,
	metrics *internal.ConsumerHandleGotoMetrics) (ok bool) {
	msgGoto, ok := checkTypeGotoMap[checkType]
	if !ok {
		return false
	}

	return l.internalDecide4Goto(msgGoto, msg, checkStatus, metrics)
}

func (l *consumeListener) internalDecide4Goto(msgGoto internal.HandleGoto, msg ConsumerMessage, checkStatus checker.CheckStatus,
	metrics *internal.ConsumerHandleGotoMetrics) (ok bool) {
	decider := l.getDeciderByGotoAction(msgGoto, msg)
	if decider == nil {
		return false
	}

	start := time.Now()
	decided := decider.Decide(msg.ConsumerMessage, checkStatus)
	latency := time.Since(start).Seconds()
	metrics.DecideLatency.Observe(latency)
	if decided {
		metrics.DecideSuccess.Inc()
	} else {
		metrics.DecideFailed.Inc()
	}
	return decided
}

func (l *consumeListener) getDeciderByGotoAction(msgGoto internal.HandleGoto, msg ConsumerMessage) internalDecider {
	lvl := msg.Level()
	switch msgGoto {
	case handler.GotoDone:
		return l.generalDeciders.doneDecider
	case handler.GotoPending:
		if l.enables.PendingEnable {
			return l.levelDeciders[lvl].pendingDecider
		}
	case handler.GotoBlocking:
		if l.enables.BlockingEnable {
			return l.levelDeciders[lvl].blockingDecider
		}
	case handler.GotoRetrying:
		if l.enables.RetryingEnable {
			return l.levelDeciders[lvl].retryingDecider
		}
	case handler.GotoDead:
		if l.enables.DeadEnable {
			return l.generalDeciders.deadDecider
		}
	case handler.GotoDiscard:
		if l.enables.DiscardEnable {
			return l.generalDeciders.discardDecider
		}
	case handler.GotoUpgrade:
		if l.enables.UpgradeEnable {
			return l.levelDeciders[lvl].upgradeDecider
		}
	case handler.GotoDegrade:
		if l.enables.DegradeEnable {
			return l.levelDeciders[lvl].degradeDecider
		}
	case internalGotoReroute:
		if l.enables.RerouteEnable {
			return l.generalDeciders.rerouteDecider
		}
	default:
		l.logger.Warnf("invalid msg goto action: %v", msgGoto)
	}
	return nil
}

func (l *consumeListener) getHandleMetrics(msg ConsumerMessage, msgGoto internal.HandleGoto) *internal.ConsumerHandleGotoMetrics {
	if metrics, ok := l.deciderMetrics.Load(msgGoto); ok {
		return metrics.(*internal.ConsumerHandleGotoMetrics)
	}
	metrics := l.client.metricsProvider.GetConsumerHandleGotoMetrics(l.logTopics, l.logLevels, msg.Topic(), msg.Level(), msg.Status(), msgGoto)
	l.deciderMetrics.Store(msgGoto, metrics)
	return metrics
}

func (l *consumeListener) Close() {
	l.closeListenerOnce.Do(func() {
		if l.leveledConsumer != nil {
			l.leveledConsumer.Close()
		}
		if l.multiLeveledConsumer != nil {
			for _, con := range l.multiLeveledConsumer.levelConsumers {
				con.Close()
			}
		}
		for _, chk := range l.checkers {
			chk.Close()
		}
		if l.generalDeciders != nil {
			l.generalDeciders.Close()
		}
		for _, hds := range l.levelDeciders {
			hds.Close()
		}
		l.logger.Info("closed consumer listener")
		l.metrics.ListenersOpened.Dec()
	})

}

// ------ helper ------

type wrappedCheckpoint struct {
	*checker.ConsumeCheckpoint
	metrics *internal.TypedCheckMetrics
}

func newWrappedCheckpoint(ckp *checker.ConsumeCheckpoint, metrics *internal.TypedCheckMetrics) *wrappedCheckpoint {
	wrappedCkp := &wrappedCheckpoint{ConsumeCheckpoint: ckp, metrics: metrics}
	wrappedCkp.metrics.CheckersOpened.Inc()
	return wrappedCkp
}

func (c *wrappedCheckpoint) Close() {
	c.metrics.CheckersOpened.Dec()
}
