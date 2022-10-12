package soften

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

type producerTransferDecider struct {
	logger          log.Logger
	client          pulsar.Client
	options         *producerTransferDeciderOptions
	routers         map[string]*router
	routersLock     sync.RWMutex
	routerMetrics   map[string]*internal.ProducerDeciderMetrics
	metricsProvider *internal.MetricsProvider
}

type producerTransferDeciderOptions struct {
	groundTopic string
	level       internal.TopicLevel
	transfer    *config.TransferPolicy
}

func newProducerTransferDecider(producer *producer, options *producerTransferDeciderOptions, metricsProvider *internal.MetricsProvider) (*producerTransferDecider, error) {
	if options == nil {
		return nil, errors.New("missing options for Transfer decider")
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

	d := &producerTransferDecider{
		logger:          producer.logger,
		client:          producer.client.Client,
		options:         options,
		routers:         make(map[string]*router),
		routerMetrics:   make(map[string]*internal.ProducerDeciderMetrics),
		metricsProvider: metricsProvider,
	}
	return d, nil
}

func (d *producerTransferDecider) Decide(ctx context.Context, msg *pulsar.ProducerMessage,
	checkStatus checker.CheckStatus) (mid pulsar.MessageID, err error, decided bool) {
	// valid check status
	if !checkStatus.IsPassed() {
		err = errors.New("transfer decider failed to execute as check status is not passed")
		return nil, err, false
	}
	// format topic
	destTopic := checkStatus.GetGotoExtra().Topic
	if destTopic == "" {
		destTopic = d.options.transfer.Topic
	}
	if destTopic == "" {
		err = errors.New("failed to transfer message because there is no topic is specified")
		return nil, err, false
	}

	if d.options.transfer.CountMode == config.CountPassNull {
		message.Helper.ClearMessageCounter(msg.Properties)
		message.Helper.ClearStatusMessageCounters(msg.Properties)
	}
	// consume time info
	message.Helper.InjectConsumeTime(msg.Properties, checkStatus.GetGotoExtra().ConsumeTime)

	// get or create router
	rtr, err := d.internalSafeGetRouter(destTopic)
	if err != nil {
		d.logger.Warnf("failed to create router for topic: %s", destTopic)
		return nil, err, false
	}
	if !rtr.ready {
		// wait router until it's ready
		if d.options.transfer.ConnectInSyncEnable {
			<-rtr.readyCh
		} else {
			// back to other router or main topic before the checked router is ready
			d.logger.Warnf("skip to decide because router is still not ready for topic: %s", destTopic)
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
		d.logger.Warnf("failed to Transfer message, payload size: %v, properties: %v", len(msg.Payload), msg.Properties)
		return mid, err, false
	}
	return mid, err, true
}

func (d *producerTransferDecider) DecideAsync(ctx context.Context, msg *pulsar.ProducerMessage, checkStatus checker.CheckStatus,
	callback func(pulsar.MessageID, *pulsar.ProducerMessage, error)) (decided bool) {
	// valid check status
	if !checkStatus.IsPassed() {
		err := errors.New("transfer decider failed to execute as check status is not passed")
		callback(nil, msg, err)
		return false
	}
	// format topic
	destTopic := checkStatus.GetGotoExtra().Topic
	if destTopic == "" {
		destTopic = d.options.transfer.Topic
	}
	if destTopic == "" {
		err := errors.New("failed to transfer message because there is no topic is specified")
		callback(nil, msg, err)
		return false
	}

	// consume time info
	message.Helper.InjectConsumeTime(msg.Properties, checkStatus.GetGotoExtra().ConsumeTime)
	// get or create router
	rtr, err := d.internalSafeGetRouter(destTopic)
	if err != nil {
		d.logger.Warnf("failed to create router for topic: %s", destTopic)
		return false
	}
	if !rtr.ready {
		// wait router until it's ready
		if d.options.transfer.ConnectInSyncEnable {
			<-rtr.readyCh
		} else {
			// back to other router or main topic before the checked router is ready
			d.logger.Warnf("skip to decide because router is still not ready for topic: %s", destTopic)
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

func (d *producerTransferDecider) internalSafeGetRouter(topic string) (*router, error) {
	d.routersLock.RLock()
	rtr, ok := d.routers[topic]
	d.routersLock.RUnlock()
	if ok {
		return rtr, nil
	}
	rtOption := routerOptions{
		Topic:               topic,
		connectInSyncEnable: d.options.transfer.ConnectInSyncEnable,
		BackoffMaxTimes:     d.options.transfer.PublishPolicy.BackoffMaxTimes,
		BackoffDelays:       d.options.transfer.PublishPolicy.BackoffDelays,
		BackoffPolicy:       d.options.transfer.PublishPolicy.BackoffPolicy,
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
		metric := d.metricsProvider.GetProducerDeciderMetrics(d.options.groundTopic, topic, decider.GotoTransfer.String())
		metric.DecidersOpened.Inc()
		d.routerMetrics[topic] = metric
		return rtr, nil
	}
}

func (d *producerTransferDecider) close() {
	for _, metric := range d.routerMetrics {
		metric.DecidersOpened.Dec()
	}
}
