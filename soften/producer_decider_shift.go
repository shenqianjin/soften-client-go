package soften

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/shenqianjin/soften-client-go/soften/support/util"
)

type producerShiftDecider struct {
	logger          log.Logger
	client          pulsar.Client
	options         *producerShiftDeciderOptions
	routers         map[string]*router
	routersLock     sync.RWMutex
	routerMetrics   map[string]*internal.ProducerDeciderMetrics
	metricsProvider *internal.MetricsProvider
}

type producerShiftDeciderOptions struct {
	groundTopic string
	level       internal.TopicLevel
	msgGoto     internal.DecideGoto
	shift       *config.ShiftPolicy
}

func newProducerShiftDecider(producer *producer, options *producerShiftDeciderOptions, metricsProvider *internal.MetricsProvider) (*producerShiftDecider, error) {
	if options == nil {
		return nil, errors.New("missing options for Transfer decider")
	}
	if options.groundTopic == "" {
		return nil, errors.New("topic is blank")
	}
	if options.level == "" {
		return nil, errors.New("level is blank")
	}
	if options.shift == nil {
		return nil, errors.New(fmt.Sprintf("missing shift policy for %v decider", options.msgGoto))
	}
	switch options.msgGoto {
	case decider.GotoUpgrade:
		if options.shift.Level != "" && options.shift.Level.OrderOf() <= options.level.OrderOf() {
			return nil, errors.New("the specified level is too lower for upgrade decider")
		}
	case decider.GotoDegrade:
		if options.shift.Level != "" && options.shift.Level.OrderOf() >= options.level.OrderOf() {
			return nil, errors.New("the specified level is too higher for degrade decider")
		}
	case decider.GotoDead:
		if options.shift.Level != "" && !strings.HasPrefix(options.shift.Level.String(), "D") {
			return nil, errors.New(fmt.Sprintf("the specified level is invalid for dead decider: %v", options.shift.Level))
		}
	case decider.GotoShift:
	default:
		return nil, errors.New(fmt.Sprintf("invalid goto for shift decider: %v", options.msgGoto))
	}

	d := &producerShiftDecider{
		logger:          producer.logger,
		client:          producer.client.Client,
		options:         options,
		routers:         make(map[string]*router),
		routerMetrics:   make(map[string]*internal.ProducerDeciderMetrics),
		metricsProvider: metricsProvider,
	}
	return d, nil
}

func (d *producerShiftDecider) Decide(ctx context.Context, msg *pulsar.ProducerMessage,
	checkStatus checker.CheckStatus) (mid pulsar.MessageID, err error, decided bool) {
	// valid check status
	if !checkStatus.IsPassed() {
		err = errors.New(fmt.Sprintf("Failed to decide message as %v because check status is not passed, message: %v",
			d.options.msgGoto, formatPayloadLogContent(msg.Payload)))
		return nil, err, false
	}
	// parse log entry
	logEntry := util.ParseLogEntry(ctx, d.logger)
	// format topic
	destTopic, err := d.internalFormatDestTopic(checkStatus, msg)
	if err != nil {
		logEntry.Error(err)
	}

	if d.options.shift.CountMode == config.CountPassNull {
		message.Helper.ClearMessageCounter(msg.Properties)
		message.Helper.ClearStatusMessageCounters(msg.Properties)
	}
	// consume time info
	message.Helper.InjectConsumeTime(msg.Properties, checkStatus.GetGotoExtra().ConsumeTime)

	// get or create router
	rtr, err := d.internalSafeGetRouter(destTopic)
	if err != nil {
		err = errors.New(fmt.Sprintf("failed to create router for topic: %s", destTopic))
		return nil, err, false
	}
	if !rtr.ready {
		// wait router until it's ready
		if d.options.shift.ConnectInSyncEnable {
			<-rtr.readyCh
		} else {
			// back to other router or main topic before the checked router is ready
			logEntry.Warnf("skip to decide because router is still not ready for topic: %s", destTopic)
			return nil, nil, false
		}
	}
	// use atomic bool to avoid race
	isDone := uint32(0)
	doneCh := make(chan struct{}, 1)
	callback := func(ID pulsar.MessageID, message *pulsar.ProducerMessage, e error) {
		if atomic.CompareAndSwapUint32(&isDone, 0, 1) {
			err = e
			mid = ID
			close(doneCh)
		}
	}
	// send
	rtr.Chan() <- &RouteMessage{
		producerMsg: msg,
		callback:    callback,
	}
	// wait for send request to finish
	<-doneCh
	if err != nil {
		d.logger.Warnf("Failed to send message to topic: %v. message: %v, err: %v",
			destTopic, formatPayloadLogContent(msg.Payload), err)
		return mid, err, false
	}
	logEntry.Warnf("Success to send message to topic: %v. message: %v", destTopic, formatPayloadLogContent(msg.Payload))
	return mid, err, true
}

func (d *producerShiftDecider) DecideAsync(ctx context.Context, msg *pulsar.ProducerMessage, checkStatus checker.CheckStatus,
	callback func(pulsar.MessageID, *pulsar.ProducerMessage, error)) (decided bool) {
	// valid check status
	if !checkStatus.IsPassed() {
		err := errors.New(fmt.Sprintf("Failed to decide message as %v because check status is not passed, message: %v",
			d.options.msgGoto, formatPayloadLogContent(msg.Payload)))
		callback(nil, msg, err)
		return false
	}
	// parse log entry
	logEntry := util.ParseLogEntry(ctx, d.logger)
	// format topic
	destTopic, err := d.internalFormatDestTopic(checkStatus, msg)
	if err != nil {
		d.logger.Error(err)
	}

	// consume time info
	message.Helper.InjectConsumeTime(msg.Properties, checkStatus.GetGotoExtra().ConsumeTime)
	// get or create router
	rtr, err := d.internalSafeGetRouter(destTopic)
	if err != nil {
		callback(nil, msg, err)
		return false
	}
	if !rtr.ready {
		// wait router until it's ready
		if d.options.shift.ConnectInSyncEnable {
			<-rtr.readyCh
		} else {
			// back to other router or main topic before the checked router is ready
			logEntry.Warnf("skip to decide because router is still not ready for topic: %s", destTopic)
			return false
		}
	}
	// send
	rtr.Chan() <- &RouteMessage{
		producerMsg: msg,
		callback:    callback,
	}

	return true
}

func (d *producerShiftDecider) internalFormatDestTopic(cs checker.CheckStatus, msg *pulsar.ProducerMessage) (string, error) {
	destLevel := cs.GetGotoExtra().Level
	if destLevel == "" {
		destLevel = d.options.shift.Level
	}
	if destLevel == "" {
		return "", errors.New(fmt.Sprintf("failed to shift (%v) message "+
			"because there is no level is specified. msg: %v", d.options.msgGoto, string(msg.Payload)))
	}
	switch d.options.msgGoto {
	case decider.GotoUpgrade:
		if destLevel.OrderOf() <= d.options.level.OrderOf() {
			return "", errors.New(fmt.Sprintf("failed to upgrade message "+
				"because the specified level is too lower. msg: %v", string(msg.Payload)))
		}
	case decider.GotoDegrade:
		if destLevel.OrderOf() >= d.options.level.OrderOf() {
			return "", errors.New(fmt.Sprintf("failed to degrade message "+
				"because the specified level is too higher. msg: %v", string(msg.Payload)))
		}
	case decider.GotoDead:
		if !strings.HasPrefix(destLevel.String(), "D") {
			return "", errors.New(fmt.Sprintf("failed to shift message "+
				"because the specified level is invalid for dead decider. msg: %v", string(msg.Payload)))
		}
	}
	return d.options.groundTopic + destLevel.TopicSuffix(), nil
}

func (d *producerShiftDecider) internalSafeGetRouter(topic string) (*router, error) {
	d.routersLock.RLock()
	rtr, ok := d.routers[topic]
	d.routersLock.RUnlock()
	if ok {
		return rtr, nil
	}
	rtOption := routerOptions{
		Topic:               topic,
		connectInSyncEnable: d.options.shift.ConnectInSyncEnable,
		publish:             d.options.shift.Publish,
	}
	d.routersLock.Lock()
	defer d.routersLock.Unlock()
	rtr, ok = d.routers[topic]
	if ok {
		return rtr, nil
	}
	if newRtr, err := newRouter(d.logger, d.client, rtOption); err != nil {
		return nil, err
	} else {
		rtr = newRtr
		d.routers[topic] = newRtr
		metric := d.metricsProvider.GetProducerDeciderMetrics(d.options.groundTopic, topic, d.options.msgGoto.String())
		metric.DecidersOpened.Inc()
		d.routerMetrics[topic] = metric
		return rtr, nil
	}
}

func (d *producerShiftDecider) close() {
	for _, r := range d.routers {
		r.close()
	}
	for _, metric := range d.routerMetrics {
		metric.DecidersOpened.Dec()
	}
}
