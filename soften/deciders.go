package soften

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/handler"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

type internalProduceDecider interface {
	Decide(ctx context.Context, msg *pulsar.ProducerMessage,
		checkStatus checker.CheckStatus) (mid pulsar.MessageID, err error, decided bool)
	DecideAsync(ctx context.Context, msg *pulsar.ProducerMessage, checkStatus checker.CheckStatus,
		callback func(pulsar.MessageID, *pulsar.ProducerMessage, error)) (decided bool)
	close()
}

type internalDecider interface {
	Decide(msg pulsar.ConsumerMessage, checkStatus checker.CheckStatus) (success bool)
	close()
}

type produceDecidersOptions struct {
	BlockingEnable bool
	PendingEnable  bool
	RetryingEnable bool
	UpgradeEnable  bool
	DegradeEnable  bool
	DeadEnable     bool
	DiscardEnable  bool
	RouteEnable    bool
	Route          *config.RoutePolicy
}

type produceDeciders map[internal.HandleGoto]internalProduceDecider

func newProduceDeciders(producer *producer, conf produceDecidersOptions) (produceDeciders, error) {
	deciders := make(produceDeciders)
	deciderOpt := routeDeciderOptions{}
	if conf.DiscardEnable {
		if err := deciders.tryLoadDecider(producer, handler.GotoDiscard, deciderOpt); err != nil {
			return nil, err
		}
	}
	if conf.DeadEnable {
		deciderOpt.connectInSyncEnable = true
		if err := deciders.tryLoadDecider(producer, handler.GotoDead, deciderOpt); err != nil {
			return nil, err
		}
	}
	if conf.BlockingEnable {
		deciderOpt.connectInSyncEnable = true
		if err := deciders.tryLoadDecider(producer, handler.GotoBlocking, deciderOpt); err != nil {
			return nil, err
		}
	}
	if conf.PendingEnable {
		deciderOpt.connectInSyncEnable = true
		if err := deciders.tryLoadDecider(producer, handler.GotoPending, deciderOpt); err != nil {
			return nil, err
		}
	}
	if conf.RetryingEnable {
		deciderOpt.connectInSyncEnable = true
		if err := deciders.tryLoadDecider(producer, handler.GotoRetrying, deciderOpt); err != nil {
			return nil, err
		}
	}
	if conf.RouteEnable {
		deciderOpt.connectInSyncEnable = conf.Route.ConnectInSyncEnable
		if err := deciders.tryLoadDecider(producer, internalGotoRoute, deciderOpt); err != nil {
			return nil, err
		}
	}
	if conf.UpgradeEnable {
		deciderOpt.connectInSyncEnable = true
		deciderOpt.upgradeLevel = producer.upgradeLevel
		if err := deciders.tryLoadDecider(producer, handler.GotoUpgrade, deciderOpt); err != nil {
			return nil, err
		}
	}
	if conf.DegradeEnable {
		deciderOpt.connectInSyncEnable = true
		deciderOpt.degradeLevel = producer.degradeLevel
		if err := deciders.tryLoadDecider(producer, handler.GotoDegrade, deciderOpt); err != nil {
			return nil, err
		}
	}
	return deciders, nil
}

func (deciders *produceDeciders) tryLoadDecider(producer *producer, msgGoto internal.HandleGoto, options routeDeciderOptions) error {
	decider, err := newRouteDecider(producer, msgGoto, &options)
	if err != nil {
		return err
	}
	(*deciders)[msgGoto] = decider
	return nil
}

// ------ general consume handlers ------

type generalConsumeDeciders struct {
	rerouteDecider internalDecider // 重路由处理器: Reroute
	deadDecider    internalDecider // 状态处理器
	doneDecider    internalDecider // 状态处理器
	discardDecider internalDecider // 状态处理器
}

type generalConsumeDeciderOptions struct {
	Topic         string                // Business Topic
	DiscardEnable bool                  // Blocking 检查开关
	DeadEnable    bool                  // Pending 检查开关
	RerouteEnable bool                  // Retrying 重试检查开关
	Reroute       *config.ReroutePolicy // Reroute Policy
}

func newGeneralConsumeDeciders(client *client, listener *consumeListener, conf generalConsumeDeciderOptions) (*generalConsumeDeciders, error) {
	handlers := &generalConsumeDeciders{}
	doneDecider, err := newFinalStatusDecider(client, listener, handler.GotoDone)
	if err != nil {
		return nil, err
	}
	handlers.doneDecider = doneDecider
	if conf.DiscardEnable {
		decider, err := newFinalStatusDecider(client, listener, handler.GotoDiscard)
		if err != nil {
			return nil, err
		}
		handlers.discardDecider = decider
	}
	if conf.DeadEnable {
		suffix := message.StatusDead.TopicSuffix()
		deadOptions := deadDecideOptions{topic: conf.Topic + suffix}
		decider, err := newDeadDecider(client, listener, deadOptions)
		if err != nil {
			return nil, err
		}
		handlers.deadDecider = decider
	}
	if conf.RerouteEnable {
		decider, err := newRerouteDecider(client, listener, conf.Reroute)
		if err != nil {
			return nil, err
		}
		handlers.rerouteDecider = decider
	}
	return handlers, nil
}

func (hds generalConsumeDeciders) Close() {
	if hds.rerouteDecider != nil {
		hds.rerouteDecider.close()
	}
	if hds.deadDecider != nil {
		hds.deadDecider.close()
	}
	if hds.doneDecider != nil {
		hds.doneDecider.close()
	}
	if hds.discardDecider != nil {
		hds.discardDecider.close()
	}
}

// ------ leveled consume handlers ------

type leveledConsumeDeciders struct {
	blockingDecider internalDecider // 状态处理器
	pendingDecider  internalDecider // 状态处理器
	retryingDecider internalDecider // 状态处理器
	upgradeDecider  internalDecider // 状态处理器: 升级为NewReady
	degradeDecider  internalDecider // 状态处理器: 升级为NewReady
}

type leveledConsumeDeciderOptions struct {
	Topic             string               // Business Topic
	Level             internal.TopicLevel  // level
	BlockingEnable    bool                 // Blocking 检查开关
	Blocking          *config.StatusPolicy // Blocking 主题检查策略
	PendingEnable     bool                 // Pending 检查开关
	Pending           *config.StatusPolicy // Pending 主题检查策略
	RetryingEnable    bool                 // Retrying 重试检查开关
	Retrying          *config.StatusPolicy // Retrying 主题检查策略
	UpgradeEnable     bool                 // 主动升级
	UpgradeTopicLevel internal.TopicLevel  // 主动升级队列级别
	DegradeEnable     bool                 // 主动降级
	DegradeTopicLevel internal.TopicLevel  // 主动升级队列级别
	//RerouteEnable     bool                  // PreReRoute 检查开关, 默认false
	//Reroute           *config.ReroutePolicy // Handle失败时的动态重路由
}

// newLeveledConsumeDeciders create handlers based on different levels.
func newLeveledConsumeDeciders(client *client, listener *consumeListener, options leveledConsumeDeciderOptions, deadHandler internalDecider) (*leveledConsumeDeciders, error) {
	deciders := &leveledConsumeDeciders{}
	if options.PendingEnable {
		suffix := message.StatusPending.TopicSuffix()
		hdOptions := statusDeciderOptions{status: message.StatusPending, msgGoto: handler.GotoPending,
			topic: options.Topic + suffix, deaDecider: deadHandler, level: options.Level}
		decider, err := newStatusDecider(client, listener, options.Pending, hdOptions)
		if err != nil {
			return nil, err
		}
		deciders.pendingDecider = decider
	}
	if options.BlockingEnable {
		suffix := message.StatusBlocking.TopicSuffix()
		hdOptions := statusDeciderOptions{status: message.StatusBlocking, msgGoto: handler.GotoBlocking,
			topic: options.Topic + suffix, deaDecider: deadHandler, level: options.Level}
		hd, err := newStatusDecider(client, listener, options.Blocking, hdOptions)
		if err != nil {
			return nil, err
		}
		deciders.blockingDecider = hd
	}
	if options.RetryingEnable {
		suffix := message.StatusRetrying.TopicSuffix()
		hdOptions := statusDeciderOptions{status: message.StatusRetrying, msgGoto: handler.GotoRetrying,
			topic: options.Topic + suffix, deaDecider: deadHandler, level: options.Level}
		decider, err := newStatusDecider(client, listener, options.Retrying, hdOptions)
		if err != nil {
			return nil, err
		}
		deciders.retryingDecider = decider
	}
	if options.UpgradeEnable {
		gradeOpts := gradeOptions{topic: options.Topic, grade2Level: options.UpgradeTopicLevel,
			level: options.Level, msgGoto: handler.GotoUpgrade}
		decider, err := newGradeDecider(client, listener, gradeOpts)
		if err != nil {
			return nil, err
		}
		deciders.upgradeDecider = decider
	}
	if options.DegradeEnable {
		gradeOpts := gradeOptions{topic: options.Topic, grade2Level: options.DegradeTopicLevel,
			level: options.Level, msgGoto: handler.GotoDegrade}
		decider, err := newGradeDecider(client, listener, gradeOpts)
		if err != nil {
			return nil, err
		}
		deciders.degradeDecider = decider
	}
	return deciders, nil
}

func (hds leveledConsumeDeciders) Close() {
	if hds.blockingDecider != nil {
		hds.blockingDecider.close()
	}
	if hds.pendingDecider != nil {
		hds.pendingDecider.close()
	}
	if hds.retryingDecider != nil {
		hds.retryingDecider.close()
	}
	if hds.upgradeDecider != nil {
		hds.upgradeDecider.close()
	}
	if hds.degradeDecider != nil {
		hds.degradeDecider.close()
	}
}
