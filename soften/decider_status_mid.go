package soften

import (
	"strconv"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

type statusDeciderOptions struct {
	topic      string                 // default ${TOPIC}_RETRYING, 固定后缀，不允许定制
	status     internal.MessageStatus // MessageStatus
	msgGoto    internal.HandleGoto    // MessageStatus
	deaDecider internalDecider        //
	level      internal.TopicLevel
}

type statusDecider struct {
	router  *reRouter
	policy  *config.StatusPolicy
	options statusDeciderOptions
	metrics *internal.ListenerDecideGotoMetrics
}

func newStatusDecider(client *client, listener *consumeListener, policy *config.StatusPolicy, options statusDeciderOptions) (*statusDecider, error) {
	rt, err := newReRouter(client.logger, client.Client, reRouterOptions{Topic: options.topic, connectInSyncEnable: true})
	if err != nil {
		return nil, err
	}
	metrics := client.metricsProvider.GetListenerLeveledDecideGotoMetrics(listener.logTopics, listener.logLevels, options.level, options.msgGoto)
	statusRouter := &statusDecider{router: rt, policy: policy, options: options, metrics: metrics}
	metrics.DecidersOpened.Inc()
	return statusRouter, nil
}

func (hd *statusDecider) Decide(msg pulsar.ConsumerMessage, cheStatus checker.CheckStatus) bool {
	if !cheStatus.IsPassed() {
		return false
	}
	statusReconsumeTimes := message.Parser.GetStatusConsumeTimes(hd.options.status, msg)
	// check to dead if exceed max status reconsume times
	if statusReconsumeTimes >= hd.policy.ConsumeMaxTimes {
		return hd.tryDeadInternal(msg)
	}
	statusReentrantTimes := message.Parser.GetStatusReentrantTimes(hd.options.status, msg)
	// check to dead if exceed max reentrant times
	if statusReentrantTimes >= hd.policy.ReentrantMaxTimes {
		return hd.tryDeadInternal(msg)
	}
	// check Nack for equal status
	delay := hd.policy.BackoffPolicy.Next(0, statusReconsumeTimes)
	if delay == 0 {
		delay = hd.policy.ReentrantDelay // default a newStatus.reentrantDelay
	} else if delay < hd.policy.ReentrantDelay { // delay equals or larger than reentrant delay is the essential condition to switch status
		msg.Consumer.NackLater(msg.Message, time.Duration(delay)*time.Second)
		return true
	}

	// prepare to re-route
	props := make(map[string]string)
	for k, v := range msg.Properties() {
		props[k] = v
	}
	// record origin information when re-route first time
	message.Helper.InjectOriginTopic(msg, &props)
	message.Helper.InjectOriginMessageId(msg, &props)
	message.Helper.InjectOriginPublishTime(msg, &props)
	message.Helper.InjectOriginLevel(msg, &props)
	message.Helper.InjectOriginStatus(msg, &props)
	// status switch
	currentStatus := message.Parser.GetCurrentStatus(msg)
	if currentStatus != hd.options.status {
		message.Helper.InjectPreviousLevel(msg, &props)
		message.Helper.InjectPreviousStatus(msg, &props)
	}
	// total consume times on the message
	xReconsumeTimes := message.Parser.GetXReconsumeTimes(msg)
	xReconsumeTimes++
	props[message.XPropertyReconsumeTimes] = strconv.Itoa(xReconsumeTimes) // initialize continuous consume times for the new msg
	// reconsume time and reentrant time
	now := time.Now()
	props[message.XPropertyReconsumeTime] = now.Add(time.Duration(delay) * time.Second).UTC().Format(internal.RFC3339TimeInSecondPattern)
	props[message.XPropertyReentrantTime] = now.Add(time.Duration(hd.policy.ReentrantDelay) * time.Second).UTC().Format(internal.RFC3339TimeInSecondPattern)
	// reconsume times for goto status
	reentrantStartRedeliveryCount := message.Parser.GetReentrantStartRedeliveryCount(msg)
	statusReconsumeTimes += int(msg.RedeliveryCount() - reentrantStartRedeliveryCount + 1) // the subtraction is the nack times in current status
	message.Helper.InjectStatusReconsumeTimes(hd.options.status, statusReconsumeTimes, &props)
	props[message.XPropertyReentrantStartRedeliveryCount] = strconv.FormatUint(uint64(msg.RedeliveryCount()), 10)
	// reentrant times for goto status
	statusReentrantTimes++
	message.Helper.InjectStatusReentrantTimes(hd.options.status, statusReentrantTimes, &props)

	producerMsg := pulsar.ProducerMessage{
		Payload:     msg.Payload(),
		Key:         msg.Key(),
		OrderingKey: msg.OrderingKey(),
		Properties:  props,
		EventTime:   msg.EventTime(),
	}
	hd.router.Chan() <- &RerouteMessage{
		consumerMsg: msg,
		producerMsg: producerMsg,
	}
	return true
}

func (hd *statusDecider) tryDeadInternal(msg pulsar.ConsumerMessage) bool {
	if hd.options.deaDecider != nil {
		return hd.options.deaDecider.Decide(msg, checker.CheckStatusPassed)
	}

	if true {
		msg.Consumer.Ack(msg.Message)
		return true
	}

	return false
}

func (hd *statusDecider) close() {
	hd.metrics.DecidersOpened.Dec()
}
