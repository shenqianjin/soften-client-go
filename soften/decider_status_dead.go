package soften

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/interceptor"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/shenqianjin/soften-client-go/soften/support/util"
	"github.com/sirupsen/logrus"
)

type deadDecideOptions struct {
	groundTopic  string
	subscription string
	level        internal.TopicLevel
	logLevel     logrus.Level

	leveledInterceptorsMap map[internal.TopicLevel]interceptor.ConsumeInterceptors
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
	if policy == nil {
		return nil, errors.New("missing policy for dead decider")
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
		Topic:               options.groundTopic + options.level.TopicSuffix() + subscriptionSuffix + message.StatusDead.TopicSuffix(),
		connectInSyncEnable: true,
		publish:             policy.Publish,
	}
	rt, err := newRouter(client.logger, client.Client, rtOptions)
	if err != nil {
		return nil, err
	}
	d := &deadDecider{router: rt, logger: client.logger, options: options, metricsProvider: metricsProvider}
	d.metricsProvider.GetListenerDecidersMetrics(d.options.groundTopic, d.options.subscription, internal.GotoDead).DecidersOpened.Inc()
	return d, nil
}

func (d *deadDecider) Decide(ctx context.Context, msg consumerMessage, decision decider.Decision) bool {
	if decision.GetGoto() != internal.GotoDead {
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
	message.Helper.InjectPreviousErrorMessage(props, decision.GetErr())

	producerMsg := pulsar.ProducerMessage{
		Payload:     msg.Payload(),
		Key:         msg.Key(),
		OrderingKey: msg.OrderingKey(),
		Properties:  props,
		EventTime:   msg.EventTime(),
	}
	callback := func(messageID pulsar.MessageID, producerMessage *pulsar.ProducerMessage, err error) {
		if err != nil {
			logEntry.WithField("msgID", msg.ID()).Errorf("Failed to decide message as dead to topic: %s, err: %v", d.router.options.Topic, err)
			msg.Consumer.Nack(msg)
			msg.internalExtra.consumerMetrics.ConsumeMessageNacks.Inc()
		} else {
			logContent := fmt.Sprintf("publish time: %v, properties: %v, payload: %v", msg.PublishTime(), msg.Properties(), formatPayloadLogContent(msg.Payload()))
			originalPublishTime := message.Parser.GetOriginalPublishTime(msg)
			if !originalPublishTime.IsZero() {
				logContent = fmt.Sprintf("%s, latency from original publish: %v", logContent, time.Now().Sub(originalPublishTime))
			}
			if !msg.EventTime().IsZero() {
				logContent = fmt.Sprintf("%s, latency from event: %v", logContent, time.Now().Sub(msg.EventTime()))
			}
			if d.options.logLevel >= logrus.WarnLevel {
				logEntry.WithField("msgID", msg.ID()).Warnf("Succeed to decide message as dead to topic: %v, message: %v", d.router.options.Topic, logContent)
			}
			msg.Ack()
			msg.internalExtra.consumerMetrics.ConsumeMessageAcks.Inc()
		}
	}
	// execute on decide interceptors
	if len(d.options.leveledInterceptorsMap[msg.Level()]) > 0 {
		d.options.leveledInterceptorsMap[msg.Level()].OnDecide(ctx, msg, decision)
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
	d.metricsProvider.GetListenerDecidersMetrics(d.options.groundTopic, d.options.subscription, internal.GotoDead).DecidersOpened.Dec()
}
