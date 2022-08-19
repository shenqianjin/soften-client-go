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
	statusReconsumeTimes := message.Parser.GetStatusReconsumeTimes(hd.options.status, msg)
	// check to dead if exceed max status reconsume times
	if statusReconsumeTimes >= hd.policy.ConsumeMaxTimes {
		return hd.tryDeadInternal(msg)
	}
	statusReentrantTimes := message.Parser.GetStatusReentrantTimes(hd.options.status, msg)
	// check to dead if exceed max reentrant times
	if statusReentrantTimes >= hd.policy.ReentrantMaxTimes {
		return hd.tryDeadInternal(msg)
	}
	currentStatus := message.Parser.GetCurrentStatus(msg)
	delay := uint(0)
	// check Nack for equal status
	if currentStatus == hd.options.status {
		delay = hd.policy.BackoffPolicy.Next(0, statusReconsumeTimes)
		if delay < hd.policy.ReentrantDelay { // delay equals or larger than reentrant delay is the essential condition to switch status
			msg.Consumer.Nack(msg.Message)
			return true
		}
	}

	// prepare to re-route
	props := make(map[string]string)
	for k, v := range msg.Properties() {
		props[k] = v
	}
	if currentStatus != hd.options.status {
		// first time to happen status switch
		previousMessageStatus := message.Parser.GetPreviousStatus(msg)
		if (previousMessageStatus == "" || previousMessageStatus == message.StatusReady) && hd.options.status != message.StatusReady {
			// record origin information when re-route first time
			if _, ok := props[message.XPropertyOriginTopic]; !ok {
				props[message.XPropertyOriginTopic] = msg.Message.Topic()
			}
			if _, ok := props[message.XPropertyOriginMessageID]; !ok {
				props[message.XPropertyOriginMessageID] = message.Parser.GetMessageId(msg)
			}
			if _, ok := props[message.XPropertyOriginPublishTime]; !ok {
				props[message.XPropertyOriginPublishTime] = msg.PublishTime().UTC().Format(internal.RFC3339TimeInSecondPattern)
			}
		}
		props[message.XPropertyPreviousMessageStatus] = string(currentStatus)
		delay = hd.policy.ReentrantDelay // default a newStatus.reentrantDelay if status switch happens
	}
	now := time.Now()
	reentrantStartRedeliveryCount := message.Parser.GetReentrantStartRedeliveryCount(msg)
	props[message.XPropertyReentrantStartRedeliveryCount] = strconv.FormatUint(uint64(msg.RedeliveryCount()), 10)

	xReconsumeTimes := message.Parser.GetXReconsumeTimes(msg)
	xReconsumeTimes++
	props[message.XPropertyReconsumeTimes] = strconv.Itoa(xReconsumeTimes) // initialize continuous consume times for the new msg

	props[message.XPropertyReconsumeTime] = now.Add(time.Duration(delay) * time.Second).UTC().Format(internal.RFC3339TimeInSecondPattern)
	props[message.XPropertyReentrantTime] = now.Add(time.Duration(hd.policy.ReentrantDelay) * time.Second).UTC().Format(internal.RFC3339TimeInSecondPattern)

	if statusReconsumeTimesHeader, ok := message.XPropertyConsumeTimes(hd.options.status); ok {
		statusReconsumeTimes += int(msg.RedeliveryCount() - reentrantStartRedeliveryCount) // the subtraction is the nack times in current status
		props[statusReconsumeTimesHeader] = strconv.Itoa(statusReconsumeTimes)
	}
	if statusReentrantTimesHeader, ok := message.XPropertyReentrantTimes(hd.options.status); ok {
		statusReentrantTimes++
		props[statusReentrantTimesHeader] = strconv.Itoa(statusReentrantTimes)
	}
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
