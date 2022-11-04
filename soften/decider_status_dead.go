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
)

type deadDecideOptions struct {
	groundTopic  string
	subscription string
	level        internal.TopicLevel
}

// deadDecider route messages to ${TOPIC}-${subscription}-DLQ 主题, 固定后缀，不允许定制
type deadDecider struct {
	router          *router
	logger          log.Logger
	options         deadDecideOptions
	metricsProvider *internal.MetricsProvider
}

func newDeadDecider(client *client, policy *config.DeadPolicy, options deadDecideOptions, metricsProvider *internal.MetricsProvider) (*deadDecider, error) {
	if options.groundTopic == "" {
		return nil, errors.New("topic is blank")
	}
	if options.subscription == "" {
		return nil, errors.New("subscription is blank")
	}
	subscriptionSuffix := "-" + options.subscription
	rtOptions := routerOptions{
		Topic:               options.groundTopic + options.level.TopicSuffix() + subscriptionSuffix + message.StatusDead.TopicSuffix(),
		connectInSyncEnable: true,
		publish:             policy.Publish,
	}
	rt, err := newRouter(client.logger, client.Client, rtOptions)
	if err != nil {
		return nil, err
	}
	d := &deadDecider{router: rt, logger: client.logger, options: options, metricsProvider: metricsProvider}
	d.metricsProvider.GetListenerDecidersMetrics(d.options.groundTopic, d.options.subscription, decider.GotoDead).DecidersOpened.Inc()
	return d, nil
}

func (d *deadDecider) Decide(ctx context.Context, msg consumerMessage, cheStatus checker.CheckStatus) bool {
	if !cheStatus.IsPassed() {
		return false
	}
	// parse log entry
	logEntry := util.ParseLogEntry(ctx, d.logger)
	// prepare to re-Transfer
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
	// record previous level/status information
	message.Helper.InjectPreviousLevel(msg.Message, props)
	message.Helper.InjectPreviousStatus(msg.Message, props)

	producerMsg := pulsar.ProducerMessage{
		Payload:     msg.Payload(),
		Key:         msg.Key(),
		OrderingKey: msg.OrderingKey(),
		Properties:  props,
		EventTime:   msg.EventTime(),
	}
	callback := func(messageID pulsar.MessageID, producerMessage *pulsar.ProducerMessage, err error) {
		if err != nil {
			logEntry.WithField("msgID", msg.ID()).Errorf("Failed to send message to topic: %s, err: %v", d.router.options.Topic, err)
			msg.Consumer.Nack(msg)
			msg.internalExtra.consumerMetrics.ConsumeMessageNacks.Inc()
		} else {
			logContent := fmt.Sprintf("Succeed to send message to dead topic: %s, publish time: %v, properties: %v, payload: %v",
				d.router.options.Topic, msg.PublishTime(), msg.Properties(), string(msg.Payload()))
			originalPublishTime := message.Parser.GetOriginalPublishTime(msg)
			if !originalPublishTime.IsZero() {
				logContent = fmt.Sprintf("%s, latency from original publish: %v", logContent, time.Now().Sub(originalPublishTime))
			}
			if !msg.EventTime().IsZero() {
				logContent = fmt.Sprintf("%s, latency from event: %v", logContent, time.Now().Sub(msg.EventTime()))
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

func (d *deadDecider) close() {
	if d.router != nil {
		d.router.close()
	}
	d.metricsProvider.GetListenerDecidersMetrics(d.options.groundTopic, d.options.subscription, decider.GotoDead).DecidersOpened.Dec()
}
