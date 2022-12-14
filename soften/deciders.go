package soften

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/decider"
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

type internalConsumeDecider interface {
	Decide(ctx context.Context, msg consumerMessage, checkStatus checker.CheckStatus) (success bool)
	close()
}

// ------ produce deciders ------

type produceDecidersOptions struct {
	groundTopic    string
	level          internal.TopicLevel
	DeadEnable     bool
	Dead           *config.ShiftPolicy
	DiscardEnable  bool
	Discard        *config.DiscardPolicy
	TransferEnable bool
	Transfer       *config.TransferPolicy
	UpgradeEnable  bool
	Upgrade        *config.ShiftPolicy
	DegradeEnable  bool
	Degrade        *config.ShiftPolicy
	ShiftEnable    bool
	Shift          *config.ShiftPolicy
}

type produceDeciders map[internal.DecideGoto]internalProduceDecider

func newProduceDeciders(p *producer, options produceDecidersOptions) (produceDeciders, error) {
	deciders := make(produceDeciders)
	if options.DiscardEnable {
		msgGoto := decider.GotoDiscard
		deciderOpt := producerFinalDeciderOptions{groundTopic: options.groundTopic,
			level: options.level, discard: options.Discard}
		if d, err := newProducerFinalDecider(p, &deciderOpt, p.metricsProvider); err != nil {
			return nil, err
		} else {
			deciders[msgGoto] = d
		}
	}
	if options.DeadEnable {
		msgGoto := decider.GotoDead
		deciderOpt := producerShiftDeciderOptions{groundTopic: options.groundTopic, level: options.level,
			msgGoto: msgGoto, shift: options.Dead}
		if d, err := newProducerShiftDecider(p, &deciderOpt, p.metricsProvider); err != nil {
			return nil, err
		} else {
			deciders[msgGoto] = d
		}
	}
	if options.TransferEnable {
		deciderOpt := producerTransferDeciderOptions{groundTopic: options.groundTopic, level: options.level, transfer: options.Transfer}
		if d, err := newProducerTransferDecider(p, &deciderOpt, p.metricsProvider); err != nil {
			return nil, err
		} else {
			deciders[decider.GotoTransfer] = d
		}
	}
	if options.UpgradeEnable {
		msgGoto := decider.GotoUpgrade
		deciderOpt := producerShiftDeciderOptions{groundTopic: options.groundTopic, level: options.level,
			msgGoto: msgGoto, shift: options.Upgrade}
		if d, err := newProducerShiftDecider(p, &deciderOpt, p.metricsProvider); err != nil {
			return nil, err
		} else {
			deciders[msgGoto] = d
		}
	}
	if options.DegradeEnable {
		msgGoto := decider.GotoDegrade
		deciderOpt := producerShiftDeciderOptions{groundTopic: options.groundTopic, level: options.level,
			msgGoto: msgGoto, shift: options.Degrade}
		if s, err := newProducerShiftDecider(p, &deciderOpt, p.metricsProvider); err != nil {
			return nil, err
		} else {
			deciders[deciderOpt.msgGoto] = s
		}
	}
	if options.ShiftEnable {
		msgGoto := decider.GotoShift
		deciderOpt := producerShiftDeciderOptions{groundTopic: options.groundTopic, level: options.level,
			msgGoto: msgGoto, shift: options.Shift}
		if s, err := newProducerShiftDecider(p, &deciderOpt, p.metricsProvider); err != nil {
			return nil, err
		} else {
			deciders[deciderOpt.msgGoto] = s
		}
	}
	return deciders, nil
}

// ------ general consume handlers ------

type generalConsumeDeciders struct {
	deadDecider     internalConsumeDecider // ???????????????
	doneDecider     internalConsumeDecider // ???????????????
	discardDecider  internalConsumeDecider // ???????????????
	upgradeDecider  internalConsumeDecider // ???????????????
	degradeDecider  internalConsumeDecider // ???????????????
	shiftDecider    internalConsumeDecider // ?????????????????????
	transferDecider internalConsumeDecider // ???????????????: ????????????????????????ground topic
}

type generalConsumeDeciderOptions struct {
	Topic            string                 // Business Topic
	Level            internal.TopicLevel    //
	subscriptionName string                 //
	Done             *config.DonePolicy     //
	DiscardEnable    bool                   // Discard ????????????
	Discard          *config.DiscardPolicy  //
	DeadEnable       bool                   // Dead ????????????
	Dead             *config.DeadPolicy     //
	TransferEnable   bool                   // Transfer ??????????????????
	Transfer         *config.TransferPolicy // Transfer Policy
	UpgradeEnable    bool                   //
	Upgrade          *config.ShiftPolicy    //
	DegradeEnable    bool                   //
	Degrade          *config.ShiftPolicy    //
	ShiftEnable      bool                   //
	Shift            *config.ShiftPolicy    //
}

func newGeneralConsumeDeciders(cli *client, l *consumeListener, options generalConsumeDeciderOptions) (*generalConsumeDeciders, error) {
	handlers := &generalConsumeDeciders{}
	doneOptions := finalStatusDeciderOptions{groundTopic: l.groundTopic, subscription: l.subscription,
		msgGoto: decider.GotoDone, done: options.Done}
	doneDecider, err := newFinalStatusDecider(l.logger, doneOptions, l.metricsProvider)
	if err != nil {
		return nil, err
	}
	handlers.doneDecider = doneDecider
	if options.DiscardEnable {
		discardOptions := finalStatusDeciderOptions{groundTopic: l.groundTopic, subscription: l.subscription,
			msgGoto: decider.GotoDiscard, discard: options.Discard}
		d, err1 := newFinalStatusDecider(l.logger, discardOptions, l.metricsProvider)
		if err1 != nil {
			return nil, err1
		}
		handlers.discardDecider = d
	}
	if options.DeadEnable {
		// dead ?????????????????????L1??????
		deadOptions := deadDecideOptions{groundTopic: l.groundTopic, level: message.L1, subscription: l.subscription}
		d, err1 := newDeadDecider(cli, options.Dead, deadOptions, l.metricsProvider)
		if err1 != nil {
			return nil, err1
		}
		handlers.deadDecider = d
	}
	if options.TransferEnable {
		deciderOpt := transferDeciderOptions{groundTopic: l.groundTopic, level: options.Level, subscription: l.subscription,
			transfer: options.Transfer}
		if d, err := newTransferDecider(cli, &deciderOpt, l.metricsProvider); err != nil {
			return nil, err
		} else {
			handlers.transferDecider = d
		}
	}
	if options.UpgradeEnable {
		msgGoto := decider.GotoUpgrade
		deciderOpt := shiftDeciderOptions{groundTopic: l.groundTopic, level: options.Level, subscription: l.subscription,
			msgGoto: msgGoto, shift: options.Upgrade}
		if d, err := newShiftDecider(cli, &deciderOpt, l.metricsProvider); err != nil {
			return nil, err
		} else {
			handlers.upgradeDecider = d
		}
	}
	if options.DegradeEnable {
		msgGoto := decider.GotoDegrade
		deciderOpt := shiftDeciderOptions{groundTopic: l.groundTopic, level: options.Level, subscription: l.subscription,
			msgGoto: msgGoto, shift: options.Degrade}
		if d, err := newShiftDecider(cli, &deciderOpt, l.metricsProvider); err != nil {
			return nil, err
		} else {
			handlers.degradeDecider = d
		}
	}
	if options.ShiftEnable {
		msgGoto := decider.GotoShift
		deciderOpt := shiftDeciderOptions{groundTopic: l.groundTopic, level: options.Level, subscription: l.subscription,
			msgGoto: msgGoto, shift: options.Shift}
		if d, err := newShiftDecider(cli, &deciderOpt, l.metricsProvider); err != nil {
			return nil, err
		} else {
			handlers.shiftDecider = d
		}
	}
	return handlers, nil
}

func (hds generalConsumeDeciders) Close() {
	if hds.transferDecider != nil {
		hds.transferDecider.close()
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
	blockingDecider *statusDecider         // ???????????????
	pendingDecider  *statusDecider         // ???????????????
	retryingDecider *statusDecider         // ???????????????
	upgradeDecider  internalConsumeDecider // ???????????????: ?????????NewReady
	degradeDecider  internalConsumeDecider // ???????????????: ?????????NewReady
}

type leveledConsumeDeciderOptions struct {
	Topic            string               // Business Topic
	subscriptionName string               //
	ConsumeMaxTimes  int                  //
	Level            internal.TopicLevel  // level
	BlockingEnable   bool                 // Blocking ????????????
	Blocking         *config.StatusPolicy // Blocking ??????????????????
	PendingEnable    bool                 // Pending ????????????
	Pending          *config.StatusPolicy // Pending ??????????????????
	RetryingEnable   bool                 // Retrying ??????????????????
	Retrying         *config.StatusPolicy // Retrying ??????????????????
	UpgradeEnable    bool                 // ????????????
	Upgrade          *config.ShiftPolicy  // ????????????????????????
	DegradeEnable    bool                 // ????????????
	Degrade          *config.ShiftPolicy  // ????????????????????????
}

// newLeveledConsumeDeciders create handlers based on different levels.
func newLeveledConsumeDeciders(cli *client, l *consumeListener, options leveledConsumeDeciderOptions, deadHandler internalConsumeDecider) (*leveledConsumeDeciders, error) {
	deciders := &leveledConsumeDeciders{}
	if options.PendingEnable {
		hdOptions := statusDeciderOptions{groundTopic: l.groundTopic, subscription: l.subscription,
			level: options.Level, consumeMaxTimes: options.ConsumeMaxTimes,
			status: message.StatusPending, msgGoto: decider.GotoPending,
			deaDecider: deadHandler}
		d, err := newStatusDecider(cli, options.Pending, hdOptions, l.metricsProvider)
		if err != nil {
			return nil, err
		}
		deciders.pendingDecider = d
	}
	if options.BlockingEnable {
		hdOptions := statusDeciderOptions{groundTopic: l.groundTopic, subscription: l.subscription,
			level: options.Level, consumeMaxTimes: options.ConsumeMaxTimes,
			status: message.StatusBlocking, msgGoto: decider.GotoBlocking,
			deaDecider: deadHandler}
		hd, err := newStatusDecider(cli, options.Blocking, hdOptions, l.metricsProvider)
		if err != nil {
			return nil, err
		}
		deciders.blockingDecider = hd
	}
	if options.RetryingEnable {
		hdOptions := statusDeciderOptions{groundTopic: l.groundTopic, subscription: l.subscription,
			level: options.Level, consumeMaxTimes: options.ConsumeMaxTimes,
			status: message.StatusRetrying, msgGoto: decider.GotoRetrying,
			deaDecider: deadHandler}
		d, err := newStatusDecider(cli, options.Retrying, hdOptions, l.metricsProvider)
		if err != nil {
			return nil, err
		}
		deciders.retryingDecider = d
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
