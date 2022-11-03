package soften

import (
	"errors"
	"fmt"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

type shiftDecider struct {
	client          *client
	logger          log.Logger
	options         *shiftDeciderOptions
	routers         map[string]*router
	routersLock     sync.RWMutex
	metricsProvider *internal.MetricsProvider
}

type shiftDeciderOptions struct {
	groundTopic  string
	level        internal.TopicLevel
	subscription string
	msgStatus    internal.MessageStatus
	msgGoto      internal.DecideGoto

	shift *config.ShiftPolicy
}

func newShiftDecider(client *client, options *shiftDeciderOptions, metricsProvider *internal.MetricsProvider) (*shiftDecider, error) {
	if options == nil {
		return nil, errors.New("missing options for shift decider")
	}
	if options.groundTopic == "" {
		return nil, errors.New("topic is blank")
	}
	if options.level == "" {
		return nil, errors.New("level is blank")
	}
	if options.msgGoto != decider.GotoUpgrade && options.msgGoto != decider.GotoDegrade && options.msgGoto != decider.GotoShift {
		return nil, errors.New(fmt.Sprintf("invalid goto decision for consumer shift decider: %v", options.msgGoto))
	}
	if options.shift == nil {
		return nil, errors.New(fmt.Sprintf("missing shift policy for %v decider", options.msgGoto))
	}
	if options.msgGoto == decider.GotoUpgrade {
		if options.shift.Level != "" && options.shift.Level.OrderOf() <= options.level.OrderOf() {
			return nil, errors.New("the specified level is too lower for upgrade decider")
		}
	} else if options.msgGoto == decider.GotoDegrade {
		if options.shift.Level != "" && options.shift.Level.OrderOf() >= options.level.OrderOf() {
			return nil, errors.New("the specified level is too higher for degrade decider")
		}
	}

	routers := make(map[string]*router)
	d := &shiftDecider{
		client:          client,
		logger:          client.logger,
		routers:         routers,
		options:         options,
		metricsProvider: metricsProvider,
	}
	d.metricsProvider.GetListenerDecidersMetrics(d.options.groundTopic, d.options.subscription, options.msgGoto).DecidersOpened.Inc()
	return d, nil
}

func (d *shiftDecider) Decide(msg consumerMessage, cheStatus checker.CheckStatus) bool {
	if !cheStatus.IsPassed() {
		return false
	}
	// format topic
	topic, err := d.internalFormatDestTopic(cheStatus, msg)
	if err != nil {
		d.logger.Error(err)
		return false
	}
	// create or get router
	rtr, err := d.internalSafeGetRouterInAsync(topic)
	if err != nil {
		d.logger.Error(err)
		return false
	}
	if !rtr.ready {
		if d.options.shift.ConnectInSyncEnable {
			<-rtr.readyCh
		} else {
			d.logger.Warnf("skip to decide because router is still not ready for topic: %s", topic)
			return false
		}
	}
	// prepare to transfer
	props := make(map[string]string)
	for k, v := range msg.Properties() {
		props[k] = v
	}
	if d.options.shift.CountMode == config.CountPassNull {
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
	// consume time info
	message.Helper.InjectConsumeTime(props, cheStatus.GetGotoExtra().ConsumeTime)

	producerMsg := pulsar.ProducerMessage{
		Payload:     msg.Payload(),
		Key:         msg.Key(),
		OrderingKey: msg.OrderingKey(),
		Properties:  props,
		EventTime:   msg.EventTime(),
	}
	callback := func(messageID pulsar.MessageID, producerMessage *pulsar.ProducerMessage, err error) {
		if err != nil {
			d.logger.WithError(err).WithField("msgID", msg.ID()).Errorf("Failed to send message to topic: %s", rtr.options.Topic)
			msg.Consumer.Nack(msg)
			msg.internalExtra.consumerMetrics.ConsumeMessageNacks.Inc()
		} else {
			d.logger.WithField("msgID", msg.ID()).Debugf("Succeed to send message to topic: %s", rtr.options.Topic)
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

func (d *shiftDecider) internalFormatDestTopic(cs checker.CheckStatus, msg consumerMessage) (string, error) {
	destLevel := cs.GetGotoExtra().Level
	if destLevel == "" {
		destLevel = d.options.shift.Level
	}
	if destLevel == "" {
		return "", errors.New(fmt.Sprintf("failed to shift (%v) message "+
			"because there is no level is specified. msgId: %v", d.options.msgGoto, msg.ID()))
	}
	if d.options.msgGoto == decider.GotoUpgrade {
		if destLevel.OrderOf() <= d.options.level.OrderOf() {
			return "", errors.New(fmt.Sprintf("failed to upgrade message "+
				"because the specified level is too lower. msgId: %v", msg.ID()))
		}
	} else if d.options.msgGoto == decider.GotoDegrade {
		if destLevel.OrderOf() >= d.options.level.OrderOf() {
			return "", errors.New(fmt.Sprintf("failed to degrade message "+
				"because the specified level is too higher. msgId: %v", msg.ID()))
		}
	} else if d.options.msgGoto == decider.GotoShift {
		if destLevel == d.options.level {
			return "", errors.New(fmt.Sprintf("failed to shift message "+
				"because the specified level is equal to the consume level. msgId: %v", msg.ID()))
		}
	} else {
		panic(fmt.Sprintf("invalid transfer decision: %v", d.options.msgGoto))
	}
	return d.options.groundTopic + destLevel.TopicSuffix(), nil
}

func (d *shiftDecider) internalSafeGetRouterInAsync(topic string) (*router, error) {
	d.routersLock.RLock()
	rtr, ok := d.routers[topic]
	d.routersLock.RUnlock()
	if ok {
		return rtr, nil
	}
	rtOption := routerOptions{
		Topic:               topic,
		connectInSyncEnable: d.options.shift.ConnectInSyncEnable,
		publishPolicy:       d.options.shift.PublishPolicy,
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

func (d *shiftDecider) close() {
	for _, r := range d.routers {
		r.close()
	}
	d.metricsProvider.GetListenerDecidersMetrics(d.options.groundTopic, d.options.subscription, d.options.msgGoto).DecidersOpened.Dec()
}
