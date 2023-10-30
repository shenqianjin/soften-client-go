package soften

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/panjf2000/ants/v2"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/handler"
	"github.com/shenqianjin/soften-client-go/soften/interceptor"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/shenqianjin/soften-client-go/soften/support/meta"
)

type Listener interface {
	// Start starts to listen
	Start(ctx context.Context, handler handler.HandleFunc) error

	// StartPremium starts to listen in premium mode.
	// Currently, handle func signature different with regular mode, it returns handler.HandleStatus
	StartPremium(ctx context.Context, handler handler.PremiumHandleFunc) error

	// Close the producer and releases resources allocated
	Close()
}

// consumeListener listens to consume all messages of one or more than one status/levels consumers.
type consumeListener struct {
	client                 *client
	logger                 log.Logger
	metricsProvider        *internal.MetricsProvider
	groundTopic            string
	subscription           string
	messageCh              chan consumerMessage // channel used to deliver message to application
	enables                *internal.ConsumeEnables
	concurrency            *config.ConcurrencyPolicy
	escapeHandler          config.EscapeHandler
	generalDeciders        *generalConsumeDeciders
	levelDeciders          map[internal.TopicLevel]*leveledConsumeDeciders
	checkers               map[checker.CheckType]*consumeCheckpointChain
	prevCheckOrders        []checker.CheckType
	postCheckOrders        []checker.CheckType
	leveledInterceptorsMap map[internal.TopicLevel]interceptor.ConsumeInterceptors
	startListenerOnce      sync.Once
	closeListenerOnce      sync.Once
	leveledConsumers       map[internal.TopicLevel]*singleLeveledConsumer
}

func newConsumeListener(cli *client, conf config.ConsumerConfig, checkpoints map[checker.CheckType][]*checker.ConsumeCheckpoint) (*consumeListener, error) {
	topicLogger := cli.logger.SubLogger(log.Fields{"ground_topic": conf.Topic})
	if len(conf.Topics) > 1 {
		logTopics := internal.TopicParser.FormatListAsAbbr(conf.Topics)
		topicLogger = cli.logger.SubLogger(log.Fields{"topics": logTopics})
	}
	l := &consumeListener{
		client:           cli,
		groundTopic:      conf.Topic,
		subscription:     conf.SubscriptionName,
		messageCh:        make(chan consumerMessage, 10),
		logger:           topicLogger.SubLogger(log.Fields{"ground_level": conf.Level}),
		concurrency:      conf.Concurrency,
		metricsProvider:  cli.metricsProvider,
		leveledConsumers: make(map[internal.TopicLevel]*singleLeveledConsumer, 0),
	}
	if len(conf.Levels) > 1 {
		logLevels := internal.TopicLevelParser.FormatList(conf.Levels)
		l.logger = l.logger.SubLogger(log.Fields{"levels": logLevels})
	}
	// collect enables
	l.enables = l.collectEnables(&conf)
	// initialize checkers
	l.checkers = l.collectCheckers(l.enables, checkpoints)
	// collect check orders
	l.prevCheckOrders, l.postCheckOrders = l.collectCheckOrders()
	// collect interceptors
	l.leveledInterceptorsMap = make(map[internal.TopicLevel]interceptor.ConsumeInterceptors, len(conf.Levels))
	for _, level := range conf.Levels {
		l.leveledInterceptorsMap[level] = conf.LevelPolicies[level].ConsumeInterceptors
	}
	// initialize general deciders
	generalHdOptions := l.formatGeneralDecidersOptions(conf.Topics[0], l.enables, &conf)
	if deciders, err := newGeneralConsumeDeciders(cli, l, generalHdOptions); err != nil {
		return nil, err
	} else {
		l.generalDeciders = deciders
	}
	// initialize level related deciders
	l.levelDeciders = make(map[internal.TopicLevel]*leveledConsumeDeciders, len(conf.Levels))
	for _, level := range conf.Levels {
		suffix := level.TopicSuffix()
		options := l.formatLeveledDecidersOptions(conf.Topics[0]+suffix, level, l.enables, &conf)
		if deciders, err := newLeveledConsumeDeciders(cli, l, options, l.generalDeciders.deadDecider); err != nil {
			return nil, err
		} else {
			l.levelDeciders[level] = deciders
		}
	}
	// initialize escape handler
	l.initEscapeHandler(&conf)
	// initialize status singleLeveledConsumer
	if len(conf.Levels) == 1 {
		level := conf.Levels[0]
		options := &singleLeveledConsumerOptions{
			Topics:                      conf.Topics,
			SubscriptionName:            conf.SubscriptionName,
			Type:                        conf.Type,
			SubscriptionInitialPosition: conf.SubscriptionInitialPosition,
			NackBackoffDelayPolicy:      conf.NackBackoffDelayPolicy,
			NackRedeliveryDelay:         conf.NackRedeliveryDelay,
			RetryEnable:                 conf.RetryEnable,
			DLQ:                         conf.DLQ,
			policy:                      conf.LevelPolicy,
			BalanceStrategy:             conf.StatusBalanceStrategy,
			MainLevel:                   conf.Levels[0],
		}
		if consumer, err := newSingleLeveledConsumer(topicLogger, cli, level, options, l.messageCh, l.levelDeciders[level]); err != nil {
			return nil, err
		} else {
			l.leveledConsumers[level] = consumer
		}
	} else {
		if consumers, err := newMultiLeveledConsumer(topicLogger, cli, &conf, l.messageCh, l.levelDeciders); err != nil {
			return nil, err
		} else {
			l.leveledConsumers = consumers.leveledConsumers
		}
	}

	l.logger.Infof("created consume listener. topics: %v", conf.Topics)
	l.metricsProvider.GetListenMetrics(l.groundTopic, l.subscription).ListenersOpened.Inc()
	return l, nil
}

func (l *consumeListener) initEscapeHandler(conf *config.ConsumerConfig) {
	if conf.EscapeHandler != nil {
		l.escapeHandler = conf.EscapeHandler
		return
	}
	switch conf.EscapeHandleType {
	case config.EscapeAsAck:
		l.escapeHandler = func(ctx context.Context, msg message.ConsumerMessage) {
			lvl := message.Parser.GetCurrentLevel(msg)
			status := message.Parser.GetCurrentStatus(msg)
			l.logger.Warnf("ack unexpected escaped msgId: %v, level: %v, status: %v, props: %v", msg.ID(), lvl, status, msg.Properties())
			err1 := msg.Consumer.Ack(msg)
			for err1 != nil { // SDK 内部错误, 重试
				err1 = msg.Consumer.Ack(msg.Message)
			}
		}
		break
	case config.EscapeAsNack:
		l.escapeHandler = func(ctx context.Context, msg message.ConsumerMessage) {
			lvl := message.Parser.GetCurrentLevel(msg)
			status := message.Parser.GetCurrentStatus(msg)
			l.logger.Warnf("nack unexpected escaped msgId: %v, level: %v, status: %v, props: %v", msg.ID(), lvl, status, msg.Properties())
			msg.Consumer.Nack(msg.Message)
		}
		break
	case config.EscapeAsPanic:
		fallthrough
	default:
		l.escapeHandler = func(ctx context.Context, msg message.ConsumerMessage) {
			lvl := message.Parser.GetCurrentLevel(msg)
			status := message.Parser.GetCurrentStatus(msg)
			l.logger.Warnf("unexpected escaped msgId: %v, level: %v, status: %v, props: %v, payload: %v",
				msg.ID(), lvl, status, msg.Properties(), msg.Payload())
			panic(fmt.Sprintf("unexpected escaped msgId: %v, level: %v, status: %v, props: %v", msg.ID(), lvl, status, msg.Properties()))
		}
	}
	return
}

func (l *consumeListener) collectEnables(conf *config.ConsumerConfig) *internal.ConsumeEnables {
	enables := internal.ConsumeEnables{
		ReadyEnable:    true,
		DeadEnable:     *conf.DeadEnable,
		DiscardEnable:  *conf.DiscardEnable,
		BlockingEnable: *conf.BlockingEnable,
		PendingEnable:  *conf.PendingEnable,
		RetryingEnable: *conf.RetryingEnable,
		TransferEnable: *conf.TransferEnable,
		UpgradeEnable:  *conf.UpgradeEnable,
		DegradeEnable:  *conf.DegradeEnable,
		ShiftEnable:    *conf.ShiftEnable,
	}
	return &enables
}

func (l *consumeListener) collectCheckers(enables *internal.ConsumeEnables, checkpointMap map[checker.CheckType][]*checker.ConsumeCheckpoint) map[checker.CheckType]*consumeCheckpointChain {
	checkers := make(map[checker.CheckType]*consumeCheckpointChain)
	if enables.TransferEnable {
		l.tryLoadConfiguredChecker(&checkers, checker.CheckTypePrevTransfer, checkpointMap)
		l.tryLoadConfiguredChecker(&checkers, checker.CheckTypePostTransfer, checkpointMap)
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
	if enables.ShiftEnable {
		l.tryLoadConfiguredChecker(&checkers, checker.CheckTypePrevShift, checkpointMap)
		l.tryLoadConfiguredChecker(&checkers, checker.CheckTypePostShift, checkpointMap)
	}
	return checkers
}

func (l *consumeListener) tryLoadConfiguredChecker(checkers *map[checker.CheckType]*consumeCheckpointChain, checkType checker.CheckType, checkpointMap map[checker.CheckType][]*checker.ConsumeCheckpoint) {
	if checkChains, ok := checkpointMap[checkType]; ok {
		options := consumeCheckpointChainOptions{
			checkType:    checkType,
			groundTopic:  l.groundTopic,
			subscription: l.subscription,
		}
		(*checkers)[checkType] = newConsumeCheckpointChain(checkChains, options, l.metricsProvider)
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

func (l *consumeListener) formatGeneralDecidersOptions(topic string, enables *internal.ConsumeEnables, conf *config.ConsumerConfig) generalConsumeDeciderOptions {
	options := generalConsumeDeciderOptions{
		Topic:            topic,
		Level:            conf.Level,
		subscriptionName: conf.SubscriptionName,
		Done:             conf.Done,
		DiscardEnable:    enables.DiscardEnable,
		Discard:          conf.Discard,
		DeadEnable:       enables.DeadEnable,
		Dead:             conf.Dead,
		TransferEnable:   enables.TransferEnable,
		Transfer:         conf.Transfer,
		UpgradeEnable:    enables.UpgradeEnable,
		Upgrade:          conf.Upgrade,
		DegradeEnable:    enables.DegradeEnable,
		Degrade:          conf.Degrade,
		ShiftEnable:      enables.ShiftEnable,
		Shift:            conf.Shift,
	}
	return options
}

func (l *consumeListener) formatLeveledDecidersOptions(topic string, level internal.TopicLevel, enables *internal.ConsumeEnables, conf *config.ConsumerConfig) leveledConsumeDeciderOptions {
	options := leveledConsumeDeciderOptions{
		Topic:            topic,
		subscriptionName: conf.SubscriptionName,
		Level:            level,
		BlockingEnable:   enables.BlockingEnable,
		Blocking:         conf.Blocking,
		PendingEnable:    enables.PendingEnable,
		Pending:          conf.Pending,
		RetryingEnable:   enables.RetryingEnable,
		Retrying:         conf.Retrying,
		UpgradeEnable:    enables.UpgradeEnable,
		Upgrade:          conf.Upgrade,
		DegradeEnable:    enables.DegradeEnable,
		Degrade:          conf.Degrade,
	}
	return options
}

func (l *consumeListener) Start(ctx context.Context, handleFunc handler.HandleFunc) error {
	// convert decider
	premiumHandler := func(ctx context.Context, msg message.Message) handler.HandleStatus {
		success, err := handleFunc(ctx, msg)
		if success {
			return handler.StatusDone
		} else {
			return handler.StatusAuto.WithErr(err)
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
		pool, onceErr := ants.NewPool(int(l.concurrency.CorePoolSize),
			ants.WithExpiryDuration(time.Duration(l.concurrency.KeepAliveTime)*time.Second),
			ants.WithPanicHandler(l.concurrency.PanicHandler),
		)
		if onceErr != nil {
			err = onceErr
			return
		}
		// listen in async
		go func() {
			l.logger.Info("started to listening...")
			l.metricsProvider.GetListenMetrics(l.groundTopic, l.subscription).ListenersRunning.Inc()
			defer l.metricsProvider.GetListenMetrics(l.groundTopic, l.subscription).ListenersRunning.Dec()
			// receive msg and then consume one by one
			l.internalStartInPool(ctx, handleFunc, pool)
			l.logger.Info("ended to listening")
		}()
	})
	return nil
}

func (l *consumeListener) internalStartInPool(ctx context.Context, handler handler.PremiumHandleFunc, pool *ants.Pool) {
	// receive msg and submit task
	for {
		select {
		case msg, ok := <-l.messageCh:
			if !ok {
				return
			}
			// record metrics
			msg.internalExtra.listenedTime = time.Now()
			if !msg.internalExtra.receivedTime.IsZero() {
				msg.internalExtra.consumerMetrics.ListenLatency.Observe(time.Since(msg.internalExtra.receivedTime).Seconds())
			}
			if !msg.EventTime().IsZero() && msg.EventTime().After(internal.EarliestEventTime) {
				msg.internalExtra.consumerMetrics.ListenLatencyFromEvent.Observe(time.Since(msg.EventTime()).Seconds())
			}

			// submit task in blocking mode, err happens only if pool is closed.
			// Namely, the 'err != nil' condition is never meet.
			cCtx := meta.AsMetas(context.Background())
			if err := pool.Submit(func() { l.consume(cCtx, handler, msg) }); err != nil {
				panic(fmt.Sprintf("submit msg failed. err: %v", err))
			}
		case <-ctx.Done():
			pool.Release()
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
			go func(msg consumerMessage) {
				// setup meta context
				cCtx := meta.AsMetas(context.Background())
				l.consume(cCtx, handler, msg)
				<-concurrencyChan
			}(msg)
		case <-ctx.Done():
			return
		}

	}
}

func (l *consumeListener) consume(ctx context.Context, handlerFunc handler.PremiumHandleFunc, msg consumerMessage) {
	if msg.internalExtra.deferFunc != nil {
		defer msg.internalExtra.deferFunc()
	}
	msg.internalExtra.prevCheckBeginTime = time.Now()
	// prev-check to handle in turn
	for _, checkType := range l.prevCheckOrders {
		if wpCheckpoint, ok := l.checkers[checkType]; ok && len(wpCheckpoint.checkChains) > 0 {
			var chkStatus checker.CheckStatus
			for ci := 0; ci < len(wpCheckpoint.checkChains); ci++ {
				chkStatus = l.internalPrevCheck(ctx, wpCheckpoint, ci, msg)
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
			if decided := l.internalDecideByPrevCheckType(ctx, msg, checkType, chkStatus); decided {
				// return to skip biz decider if check handle succeeded
				return
			}
		}
	}

	// do handle
	start := time.Now()
	bizHandleStatus := handlerFunc(ctx, msg)
	latency := time.Now().Sub(start).Seconds()
	msgCounter := message.Parser.GetMessageCounter(msg.Message)
	metrics := l.metricsProvider.GetListenerHandleMetrics(l.groundTopic, msg.Level(), msg.Status(),
		l.subscription, msg.Topic(), bizHandleStatus.GetGoto())
	metrics.HandleLatency.Observe(latency)
	metrics.HandleConsumeTimes.Observe(float64(msgCounter.ConsumeTimes))

	// post-check to Transfer - for obvious goto action
	if bizHandleStatus.GetGoto() != "" {
		gotoCheckStatus := checker.CheckStatusPassed.WithGotoExtra(bizHandleStatus.GetGotoExtra())
		if decided := l.internalDecide4Goto(ctx, bizHandleStatus.GetGoto(), msg, gotoCheckStatus); decided {
			// return if handle succeeded
			return
		}
	}

	// post-check to Transfer - for obvious checkers or configured checkers
	postCheckTypesInTurn := l.postCheckOrders
	if len(bizHandleStatus.GetCheckTypes()) > 0 {
		postCheckTypesInTurn = bizHandleStatus.GetCheckTypes()
	}
	for _, checkType := range postCheckTypesInTurn {
		if wpCheckpoint, ok := l.checkers[checkType]; ok && len(wpCheckpoint.checkChains) > 0 {
			var chkStatus checker.CheckStatus
			for ci := 0; ci < len(wpCheckpoint.checkChains); ci++ {
				chkStatus = l.internalPostCheck(ctx, wpCheckpoint, ci, msg, bizHandleStatus.GetErr())
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
			if decided := l.internalDecideByPostCheckType(ctx, msg, checkType, chkStatus); decided {
				// return if check handle succeeded
				return
			}
		}
	}

	// here means the msg escapes to Ack/Nack, it will redeliver until the connection disconnects.
	// application is responsible for avoiding this happening
	msg.internalExtra.consumerMetrics.ConsumeMessageEscape.Inc()
	if l.escapeHandler != nil {
		l.escapeHandler(ctx, message.ConsumerMessage{Consumer: msg.Consumer, Message: msg.Message})
	}

	return
}

func (l *consumeListener) internalPrevCheck(ctx context.Context, chain *consumeCheckpointChain, chainIndex int, msg consumerMessage) checker.CheckStatus {
	// execute check chains
	start := time.Now()
	checkpoint := chain.checkChains[chainIndex]
	checkStatus := checkpoint.Prev(ctx, msg)

	// observe metrics
	latency := time.Now().Sub(start).Seconds()
	metrics := l.metricsProvider.GetListenerCheckerMetrics(chain.options.groundTopic, msg.Level(), msg.Status(),
		chain.options.subscription, msg.Topic(), chain.options.checkType.String())
	metrics.CheckLatency.Observe(latency)
	if checkStatus.IsPassed() {
		metrics.CheckPassed.Inc()
	} else {
		metrics.CheckRejected.Inc()
	}
	return checkStatus
}

func (l *consumeListener) internalPostCheck(ctx context.Context, chain *consumeCheckpointChain, ci int, msg consumerMessage, err error) checker.CheckStatus {
	// execute check chains
	start := time.Now()
	checkpoint := chain.checkChains[ci]
	checkStatus := checkpoint.Post(ctx, msg, err)

	// observe metrics
	latency := time.Now().Sub(start).Seconds()
	metrics := l.metricsProvider.GetListenerCheckerMetrics(chain.options.groundTopic, msg.Level(), msg.Status(),
		chain.options.subscription, msg.Topic(), chain.options.checkType.String())
	metrics.CheckLatency.Observe(latency)
	if checkStatus.IsPassed() {
		metrics.CheckPassed.Inc()
	} else {
		metrics.CheckRejected.Inc()
	}
	return checkStatus
}

func (l *consumeListener) internalDecideByPrevCheckType(ctx context.Context, msg consumerMessage, checkType checker.CheckType, checkStatus checker.CheckStatus) (ok bool) {
	msgGoto, ok := checkTypeGotoMap[checkType]
	if !ok {
		return false
	}
	return l.internalDecide4Goto(ctx, msgGoto, msg, checkStatus)
}

func (l *consumeListener) internalDecideByPostCheckType(ctx context.Context, msg consumerMessage, checkType checker.CheckType, checkStatus checker.CheckStatus) (ok bool) {
	msgGoto, ok := checkTypeGotoMap[checkType]
	if !ok {
		return false
	}

	return l.internalDecide4Goto(ctx, msgGoto, msg, checkStatus)
}

func (l *consumeListener) internalDecide4Goto(ctx context.Context, msgGoto internal.DecideGoto, msg consumerMessage, checkStatus checker.CheckStatus) (ok bool) {
	d := l.getDeciderByGotoAction(msgGoto, msg)
	if d == nil {
		return false
	}
	// fill deciderMetrics
	if msg.internalExtra.deciderMetrics == nil {
		metrics := l.metricsProvider.GetListenerDecideMetrics(l.groundTopic, msg.Level(), msg.Status(),
			l.subscription, msg.Topic(), msgGoto)
		msg.internalExtra.deciderMetrics = metrics
	}
	// fill messageMetrics
	if msg.internalExtra.messagesEndMetrics == nil {
		metrics := l.metricsProvider.GetListenerMessagesMetrics(l.groundTopic, msg.Level(), msg.Status(),
			l.subscription, msg.Topic(), msgGoto)
		msg.internalExtra.messagesEndMetrics = metrics

	}
	// do decider
	start := time.Now()
	decided := d.Decide(ctx, msg, checkStatus)
	now := time.Now()
	msg.internalExtra.deciderMetrics.DecideLatency.Observe(now.Sub(start).Seconds())
	if !msg.internalExtra.receivedTime.IsZero() {
		msg.internalExtra.deciderMetrics.DecideLatencyFromReceive.Observe(now.Sub(msg.internalExtra.receivedTime).Seconds())
	}
	if !msg.internalExtra.listenedTime.IsZero() {
		msg.internalExtra.deciderMetrics.DecideLatencyFromListen.Observe(now.Sub(msg.internalExtra.listenedTime).Seconds())
	}
	if !msg.internalExtra.prevCheckBeginTime.IsZero() {
		msg.internalExtra.deciderMetrics.DecicdeLatencyFromLPCheck.Observe(now.Sub(msg.internalExtra.prevCheckBeginTime).Seconds())
	}
	if decided {
		msg.internalExtra.deciderMetrics.DecideSuccess.Inc()
	} else {
		msg.internalExtra.deciderMetrics.DecideFailed.Inc()
	}
	return decided
}

func (l *consumeListener) getDeciderByGotoAction(msgGoto internal.DecideGoto, msg consumerMessage) internalConsumeDecider {
	lvl := msg.Level()
	switch msgGoto {
	case decider.GotoDone:
		return l.generalDeciders.doneDecider
	case decider.GotoPending:
		if l.enables.PendingEnable {
			return l.levelDeciders[lvl].pendingDecider
		}
	case decider.GotoBlocking:
		if l.enables.BlockingEnable {
			return l.levelDeciders[lvl].blockingDecider
		}
	case decider.GotoRetrying:
		if l.enables.RetryingEnable {
			return l.levelDeciders[lvl].retryingDecider
		}
	case decider.GotoDead:
		if l.enables.DeadEnable {
			return l.generalDeciders.deadDecider
		}
	case decider.GotoDiscard:
		if l.enables.DiscardEnable {
			return l.generalDeciders.discardDecider
		}
	case decider.GotoUpgrade:
		if l.enables.UpgradeEnable {
			return l.generalDeciders.upgradeDecider
		}
	case decider.GotoDegrade:
		if l.enables.DegradeEnable {
			return l.generalDeciders.degradeDecider
		}
	case decider.GotoShift:
		if l.enables.ShiftEnable {
			return l.generalDeciders.shiftDecider
		}
	case decider.GotoTransfer:
		if l.enables.TransferEnable {
			return l.generalDeciders.transferDecider
		}
	default:
		l.logger.Warnf("invalid msg goto action: %v", msgGoto)
	}
	return nil
}

func (l *consumeListener) Close() {
	l.closeListenerOnce.Do(func() {
		if l.leveledConsumers != nil {
			for _, cs := range l.leveledConsumers {
				cs.Close()
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
		l.metricsProvider.GetListenMetrics(l.groundTopic, l.subscription).ListenersOpened.Dec()
	})

}

// ------ helper ------

type consumeCheckpointChain struct {
	checkChains     []*checker.ConsumeCheckpoint
	options         consumeCheckpointChainOptions
	metricsProvider *internal.MetricsProvider
}

type consumeCheckpointChainOptions struct {
	checkType checker.CheckType

	groundTopic  string
	subscription string
}

func newConsumeCheckpointChain(checkChains []*checker.ConsumeCheckpoint, options consumeCheckpointChainOptions,
	metricsProvider *internal.MetricsProvider) *consumeCheckpointChain {
	c := &consumeCheckpointChain{checkChains: checkChains, options: options, metricsProvider: metricsProvider}
	c.metricsProvider.GetListenerCheckersMetrics(c.options.groundTopic, c.options.subscription, c.options.checkType.String()).CheckersOpened.Inc()
	return c
}

func (c *consumeCheckpointChain) Close() {
	c.metricsProvider.GetListenerCheckersMetrics(c.options.groundTopic, c.options.subscription, c.options.checkType.String()).CheckersOpened.Dec()
}
