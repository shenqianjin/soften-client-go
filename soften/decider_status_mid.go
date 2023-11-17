package soften

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/shenqianjin/soften-client-go/soften/support/util"
	"github.com/sirupsen/logrus"
)

type statusDeciderOptions struct {
	groundTopic     string
	subscription    string
	status          internal.MessageStatus // MessageStatus
	msgGoto         internal.DecideGoto    // MessageStatus
	deaDecider      internalConsumeDecider //
	level           internal.TopicLevel
	consumeMaxTimes int
	logLevel        logrus.Level
}

// statusDecider routes messages to ${TOPIC}-${subscription}-${status} 主题, 固定后缀，不允许定制
type statusDecider struct {
	router          *router
	logger          log.Logger
	policy          *config.StatusPolicy
	options         statusDeciderOptions
	metricsProvider *internal.MetricsProvider
}

func newStatusDecider(client *client, policy *config.StatusPolicy, options statusDeciderOptions, metricsProvider *internal.MetricsProvider) (*statusDecider, error) {
	if options.groundTopic == "" {
		return nil, errors.New("topic is blank")
	}
	if options.subscription == "" {
		return nil, errors.New("subscription is blank")
	}
	if options.status == "" {
		return nil, errors.New("status is blank")
	}
	if policy == nil {
		return nil, errors.New(fmt.Sprintf("missing policy for %v decider", options.status))
	}
	if policy.LogLevel != "" {
		if logLvl, err := logrus.ParseLevel(policy.LogLevel); err != nil {
			return nil, err
		} else {
			options.logLevel = logLvl
		}
	}
	subscriptionSuffix := "-" + options.subscription
	rtOptions := routerOptions{
		Topic:               options.groundTopic + options.level.TopicSuffix() + subscriptionSuffix + options.status.TopicSuffix(),
		connectInSyncEnable: true,
		publish:             policy.Publish,
	}
	rt, err := newRouter(client.logger, client.Client, rtOptions)
	if err != nil {
		return nil, err
	}
	d := &statusDecider{router: rt, logger: client.logger, policy: policy, options: options, metricsProvider: metricsProvider}
	d.metricsProvider.GetListenerDecidersMetrics(d.options.groundTopic, d.options.subscription, d.options.msgGoto).DecidersOpened.Inc()
	return d, nil
}

func (d *statusDecider) Decide(ctx context.Context, msg consumerMessage, decision decider.Decision) bool {
	if decision.GetGoto() != d.options.msgGoto {
		return false
	}
	statusMessageCounter := message.Parser.GetStatusMessageCounter(d.options.status, msg.Message)
	// check to dead if exceed max status reconsume times
	if *d.policy.ConsumeMaxTimes > 0 && statusMessageCounter.ConsumeReckonTimes >= int(*d.policy.ConsumeMaxTimes) {
		return d.tryDeadInternal(ctx, msg, decision)
	}
	msgCounter := message.Parser.GetMessageCounter(msg.Message)
	// check to dead if exceed max total reconsume times
	if d.options.consumeMaxTimes > 0 && msgCounter.ConsumeReckonTimes >= d.options.consumeMaxTimes {
		return d.tryDeadInternal(ctx, msg, decision)
	}
	// check Nack for equal status
	delay := d.policy.BackoffDelayPolicy.Next(0, statusMessageCounter.ConsumeTimes)
	if delay <= 0 {
		// default delay as one reentrant delay
		delay = *d.policy.ReentrantDelay
	} else if delay < *d.policy.ReentrantDelay {
		// TODO: Use Nack Later to reduce reentrant times in the future.
		// delay equals or larger than reentrant delay is the essential condition to switch status
		/*msg.Consumer.NackLater(msg.Message, time.Duration(delay)*time.Second)
		msg.internalExtra.consumerMetrics.ConsumeMessageNacks.Inc()
		return true*/
		// Why not Nack later currently?
		// (1) no redelivery count if subscription type is Exclusive or Fail over. see: https://github.com/apache/pulsar/issues/15836
		// (2) how to distinct Nack times is retrying, blocking or pending?
		delay = *d.policy.ReentrantDelay
	}

	// prepare to reentrant
	props := make(map[string]string)
	for k, v := range msg.Properties() {
		props[k] = v
	}
	// record origin information when re-Transfer first time
	message.Helper.InjectOriginTopic(msg.Message, props)
	message.Helper.InjectOriginMessageId(msg.Message, props)
	message.Helper.InjectOriginPublishTime(msg.Message, props)
	message.Helper.InjectOriginLevel(msg.Message, props)
	message.Helper.InjectOriginStatus(msg.Message, props)
	// status switch
	currentStatus := message.Parser.GetCurrentStatus(msg.Message)
	if currentStatus != d.options.status {
		message.Helper.InjectPreviousLevel(msg.Message, props)
		message.Helper.InjectPreviousStatus(msg.Message, props)
	}
	message.Helper.InjectPreviousErrorMessage(props, decision.GetErr())
	// consume time and reentrant time
	now := time.Now()
	message.Helper.InjectConsumeTime(props, now.Add(time.Duration(delay)*time.Second))
	// reentrant
	return d.Reentrant(ctx, msg, props, decision)
}

func (d *statusDecider) Reentrant(ctx context.Context, msg consumerMessage, props map[string]string, decision decider.Decision) bool {
	statusMsgCounter := message.Parser.GetStatusMessageCounter(d.options.status, msg.Message)
	// check to dead if exceed max reentrant times
	if *d.policy.ReentrantMaxTimes > 0 && statusMsgCounter.PublishTimes >= int(*d.policy.ReentrantMaxTimes) {
		return d.tryDeadInternal(ctx, msg, decision)
	}
	message.Helper.InjectReentrantTime(props, time.Now().Add(time.Duration(*d.policy.ReentrantDelay)*time.Second))
	// increase status reentrant times
	statusMsgCounter.PublishTimes++
	message.Helper.InjectStatusMessageCounter(props, d.options.status, statusMsgCounter)
	// increase total reentrant times
	msgCounter := message.Parser.GetMessageCounter(msg.Message)
	msgCounter.PublishTimes++
	message.Helper.InjectMessageCounter(props, msgCounter)

	producerMsg := pulsar.ProducerMessage{
		Payload:     msg.Payload(),
		Key:         msg.Key(),
		OrderingKey: msg.OrderingKey(),
		Properties:  props,
		EventTime:   msg.EventTime(),
	}
	// parse log entry
	logEntry := util.ParseLogEntry(ctx, d.logger)
	callback := func(messageID pulsar.MessageID, producerMessage *pulsar.ProducerMessage, err error) {
		if err != nil {
			logEntry.WithField("msgID", msg.ID()).Errorf("Failed to decide message as %v to topic: %s, err: %v",
				d.options.msgGoto, d.router.options.Topic, err)
			msg.Consumer.Nack(msg)
			msg.internalExtra.consumerMetrics.ConsumeMessageNacks.Inc()
		} else {
			if d.options.logLevel >= logrus.InfoLevel {
				logEntry.WithField("msgID", msg.ID()).Infof("Succeed to decide message as %v to topic: %s",
					d.options.msgGoto, d.router.options.Topic)
			}
			msg.Ack()
			msg.internalExtra.consumerMetrics.ConsumeMessageAcks.Inc()
		}
	}
	d.router.Chan() <- &RouteMessage{
		producerMsg: &producerMsg,
		callback:    callback,
	}
	return true
}

func (d *statusDecider) tryDeadInternal(ctx context.Context, msg consumerMessage, decision decider.Decision) bool {
	if d.options.deaDecider != nil {
		checkStatus := checker.CheckStatusPassed
		if decision != nil {
			checkStatus = checkStatus.WithErr(decision.GetErr()).WithGotoExtra(decision.GetGotoExtra())
		} else if errMsg := message.Parser.GetPreviousErrorMessage(msg); errMsg != "" { // use previous err for consume reentrant
			checkStatus.WithErr(fmt.Errorf(errMsg))
		}
		return d.options.deaDecider.Decide(ctx, msg, newDecisionByCheckStatus(internal.GotoDead, checkStatus))
	}
	return false
}

func (d *statusDecider) close() {
	if d.router != nil {
		d.router.close()
	}
	d.metricsProvider.GetListenerDecidersMetrics(d.options.groundTopic, d.options.subscription, d.options.msgGoto).DecidersOpened.Dec()
}
