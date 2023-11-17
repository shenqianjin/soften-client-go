package soften

import (
	"context"
	"errors"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/shenqianjin/soften-client-go/soften/support/util"
	"github.com/sirupsen/logrus"
)

type transferDecider struct {
	client          *client
	logger          log.Logger
	options         *transferDeciderOptions
	routers         map[string]*router
	routersLock     sync.RWMutex
	metricsProvider *internal.MetricsProvider
}

type transferDeciderOptions struct {
	groundTopic  string
	level        internal.TopicLevel
	subscription string
	transfer     *config.TransferPolicy
	logLevel     logrus.Level
}

func newTransferDecider(client *client, options *transferDeciderOptions, metricsProvider *internal.MetricsProvider) (*transferDecider, error) {
	if options == nil {
		return nil, errors.New("missing options for transfer decider")
	}
	if options.groundTopic == "" {
		return nil, errors.New("topic is blank")
	}
	if options.level == "" {
		return nil, errors.New("level is blank")
	}
	if options.transfer == nil {
		return nil, errors.New("missing transfer policy for consumer decider")
	}
	if options.transfer.LogLevel != "" {
		if logLvl, err := logrus.ParseLevel(options.transfer.LogLevel); err != nil {
			return nil, err
		} else {
			options.logLevel = logLvl
		}
	}

	routers := make(map[string]*router)
	d := &transferDecider{
		client:          client,
		logger:          client.logger,
		routers:         routers,
		options:         options,
		metricsProvider: metricsProvider,
	}
	d.metricsProvider.GetListenerDecidersMetrics(d.options.groundTopic, d.options.subscription, internal.GotoTransfer).DecidersOpened.Inc()
	return d, nil
}

func (d *transferDecider) Decide(ctx context.Context, msg consumerMessage, decision decider.Decision) bool {
	if decision.GetGoto() != internal.GotoTransfer {
		return false
	}
	// parse log entry
	logEntry := util.ParseLogEntry(ctx, d.logger)
	// format destTopic
	destTopic := decision.GetGotoExtra().Topic
	if destTopic == "" {
		destTopic = d.options.transfer.Topic
	}
	if destTopic == "" {
		logEntry.Warnf("failed to transfer message because there is no topic is specified. msgId: %v", msg.ID())
		return false
	}

	// create or get router
	rtr, err := d.internalSafeGetRouterInAsync(destTopic)
	if err != nil {
		return false
	}
	if !rtr.ready {
		if d.options.transfer.ConnectInSyncEnable {
			<-rtr.readyCh
		} else {
			logEntry.Warnf("skip to decide message as transfer to %v because router is still not ready. msgId: %v",
				destTopic, msg.ID())
			return false
		}
	}
	// prepare to transfer
	props := make(map[string]string)
	for k, v := range msg.Properties() {
		props[k] = v
	}
	if d.options.transfer.CountMode == config.CountPassNull {
		message.Helper.ClearMessageCounter(props)
		message.Helper.ClearStatusMessageCounters(props)
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
	// consume time info
	message.Helper.InjectConsumeTime(props, decision.GetGotoExtra().ConsumeTime)

	producerMsg := pulsar.ProducerMessage{
		Payload:     msg.Payload(),
		Key:         msg.Key(),
		OrderingKey: msg.OrderingKey(),
		Properties:  props,
		EventTime:   msg.EventTime(),
	}
	callback := func(messageID pulsar.MessageID, producerMessage *pulsar.ProducerMessage, err error) {
		if err != nil {
			logEntry.WithField("msgID", msg.ID()).Errorf("Failed to decide message as transfer to topic: %s, err: %v",
				rtr.options.Topic, err)
			msg.Consumer.Nack(msg)
			msg.internalExtra.consumerMetrics.ConsumeMessageNacks.Inc()
		} else {
			if d.options.logLevel >= logrus.InfoLevel {
				logEntry.WithField("msgID", msg.ID()).Infof("Succeed to decide message as transfer to topic: %s", rtr.options.Topic)
			}
			msg.Ack()
			msg.internalExtra.consumerMetrics.ConsumeMessageAcks.Inc()
		}
	}
	rtr.Chan() <- &RouteMessage{
		producerMsg: &producerMsg,
		callback:    callback,
	}
	return true
}

func (d *transferDecider) internalSafeGetRouterInAsync(topic string) (*router, error) {
	d.routersLock.RLock()
	rtr, ok := d.routers[topic]
	d.routersLock.RUnlock()
	if ok {
		return rtr, nil
	}
	rtOption := routerOptions{
		Topic:               topic,
		connectInSyncEnable: d.options.transfer.ConnectInSyncEnable,
		publish:             d.options.transfer.Publish,
	}
	d.routersLock.Lock()
	defer d.routersLock.Unlock()
	rtr, ok = d.routers[topic]
	if ok {
		return rtr, nil
	}
	if newRtr, err := newRouter(d.logger, d.client.Client, rtOption); err != nil {
		return nil, err
	} else {
		rtr = newRtr
		d.routers[topic] = newRtr
		return rtr, nil
	}
}

func (d *transferDecider) close() {
	for _, r := range d.routers {
		r.close()
	}
	d.metricsProvider.GetListenerDecidersMetrics(d.options.groundTopic, d.options.subscription, internal.GotoTransfer).DecidersOpened.Dec()
}
