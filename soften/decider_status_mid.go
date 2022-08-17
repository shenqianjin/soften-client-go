package soften

import (
	"errors"
	"strconv"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

type statusDeciderOptions struct {
	groundTopic  string
	subscription string
	status       internal.MessageStatus // MessageStatus
	msgGoto      internal.DecideGoto    // MessageStatus
	deaDecider   internalConsumeDecider //
	level        internal.TopicLevel
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
	subscriptionSuffix := "-" + options.subscription
	rtOptions := routerOptions{
		Topic:               options.groundTopic + policy.TransferLevel.TopicSuffix() + subscriptionSuffix + options.status.TopicSuffix(),
		connectInSyncEnable: true,
	}
	rt, err := newRouter(client.logger, client.Client, rtOptions)
	if err != nil {
		return nil, err
	}
	d := &statusDecider{router: rt, logger: client.logger, policy: policy, options: options, metricsProvider: metricsProvider}
	d.metricsProvider.GetListenerDecidersMetrics(d.options.groundTopic, d.options.subscription, d.options.msgGoto).DecidersOpened.Inc()
	return d, nil
}

func (d *statusDecider) Decide(msg consumerMessage, cheStatus checker.CheckStatus) bool {
	if !cheStatus.IsPassed() {
		return false
	}
	statusReconsumeTimes := message.Parser.GetStatusConsumeTimes(d.options.status, msg.Message)
	// check to dead if exceed max status reconsume times
	if statusReconsumeTimes >= d.policy.ConsumeMaxTimes {
		return d.tryDeadInternal(msg)
	}
	statusReentrantTimes := message.Parser.GetStatusReentrantTimes(d.options.status, msg.Message)
	// check to dead if exceed max reentrant times
	if statusReentrantTimes >= d.policy.ReentrantMaxTimes {
		return d.tryDeadInternal(msg)
	}
	// check Nack for equal status
	delay := d.policy.BackoffPolicy.Next(0, statusReconsumeTimes)
	if delay == 0 {
		delay = d.policy.ReentrantDelay // default a newStatus.reentrantDelay
	} else if delay < d.policy.ReentrantDelay { // delay equals or larger than reentrant delay is the essential condition to switch status
		msg.Consumer.NackLater(msg.Message, time.Duration(delay)*time.Second)
		msg.internalExtra.consumerMetrics.ConsumeMessageNacks.Inc()
		return true
	}

	// prepare to re-Transfer
	props := make(map[string]string)
	for k, v := range msg.Properties() {
		props[k] = v
	}
	// record origin information when re-Transfer first time
	message.Helper.InjectOriginTopic(msg.Message, &props)
	message.Helper.InjectOriginMessageId(msg.Message, &props)
	message.Helper.InjectOriginPublishTime(msg.Message, &props)
	message.Helper.InjectOriginLevel(msg.Message, &props)
	message.Helper.InjectOriginStatus(msg.Message, &props)
	// status switch
	currentStatus := message.Parser.GetCurrentStatus(msg.Message)
	if currentStatus != d.options.status {
		message.Helper.InjectPreviousLevel(msg.Message, &props)
		message.Helper.InjectPreviousStatus(msg.Message, &props)
	}
	// total consume times on the message
	xReconsumeTimes := message.Parser.GetReconsumeTimes(msg.Message)
	xReconsumeTimes++
	props[message.XPropertyReconsumeTimes] = strconv.Itoa(xReconsumeTimes) // initialize continuous consume times for the new msg
	// reconsume time and reentrant time
	now := time.Now()
	message.Helper.InjectConsumeTime(&props, now.Add(time.Duration(delay)*time.Second))
	message.Helper.InjectReentrantTime(&props, now.Add(time.Duration(d.policy.ReentrantDelay)*time.Second))
	// reconsume times for goto status
	reentrantStartRedeliveryCount := message.Parser.GetReentrantStartRedeliveryCount(msg.Message)
	statusReconsumeTimes += int(msg.RedeliveryCount() - reentrantStartRedeliveryCount + 1) // the subtraction is the nack times in current status
	message.Helper.InjectStatusReconsumeTimes(d.options.status, statusReconsumeTimes, &props)
	props[message.XPropertyReentrantStartRedeliveryCount] = strconv.FormatUint(uint64(msg.RedeliveryCount()), 10)
	// reentrant times for goto status
	statusReentrantTimes++
	message.Helper.InjectStatusReentrantTimes(d.options.status, statusReentrantTimes, &props)

	producerMsg := pulsar.ProducerMessage{
		Payload:     msg.Payload(),
		Key:         msg.Key(),
		OrderingKey: msg.OrderingKey(),
		Properties:  props,
		EventTime:   msg.EventTime(),
	}
	callback := func(messageID pulsar.MessageID, producerMessage *pulsar.ProducerMessage, err error) {
		if err != nil {
			d.logger.WithError(err).WithField("msgID", msg.ID()).Errorf("Failed to send message to topic: %s", d.router.options.Topic)
			msg.Consumer.Nack(msg)
			msg.internalExtra.consumerMetrics.ConsumeMessageNacks.Inc()
		} else {
			d.logger.WithField("msgID", msg.ID()).Debugf("Succeed to send message to topic: %s", d.router.options.Topic)
			msg.Consumer.Ack(msg)
			msg.internalExtra.consumerMetrics.ConsumeMessageAcks.Inc()
		}
	}
	d.router.Chan() <- &RouteMessage{
		producerMsg: &producerMsg,
		callback:    callback,
	}
	return true
}

func (d *statusDecider) tryDeadInternal(msg consumerMessage) bool {
	if d.options.deaDecider != nil {
		return d.options.deaDecider.Decide(msg, checker.CheckStatusPassed)
	}

	if true {
		msg.Consumer.Ack(msg.Message)
		return true
	}

	return false
}

func (d *statusDecider) close() {
	d.metricsProvider.GetListenerDecidersMetrics(d.options.groundTopic, d.options.subscription, d.options.msgGoto).DecidersOpened.Dec()
}
