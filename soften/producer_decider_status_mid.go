package soften

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

type producerStatusDeciderOptions struct {
	groundTopic string
	level       internal.TopicLevel
	msgStatus   internal.MessageStatus
	msgGoto     internal.DecideGoto
}

type producerStatusDecider struct {
	logger          log.Logger
	router          *router
	options         *producerStatusDeciderOptions
	metricsProvider *internal.MetricsProvider
}

func newProducerStatusDecider(producer *producer, options *producerStatusDeciderOptions, metricsProvider *internal.MetricsProvider) (*producerStatusDecider, error) {
	if options == nil {
		return nil, errors.New("missing options for Transfer decider")
	}
	if options.groundTopic == "" {
		return nil, errors.New("topic is blank")
	}
	if options.level == "" {
		return nil, errors.New("level is blank")
	}
	if options.msgGoto != decider.GotoRetrying && options.msgGoto != decider.GotoPending && options.msgGoto != decider.GotoBlocking {
		return nil, errors.New(fmt.Sprintf("invalid goto decision for producer middle status decider: %v", options.msgGoto))
	}
	if options.msgStatus != message.StatusRetrying && options.msgStatus != message.StatusPending && options.msgStatus != message.StatusBlocking {
		return nil, errors.New(fmt.Sprintf("invalid message status for producer middle status decider: %v", options.msgStatus))
	}

	topic := options.groundTopic + options.level.TopicSuffix() + options.msgStatus.TopicSuffix()
	rtOptions := routerOptions{
		Topic:               topic,
		connectInSyncEnable: true,
	}
	rt, err := newRouter(producer.logger, producer.client.Client, rtOptions)
	if err != nil {
		return nil, err
	}
	d := &producerStatusDecider{
		logger:          producer.logger.SubLogger(log.Fields{"goto": decider.GotoDiscard}),
		router:          rt,
		options:         options,
		metricsProvider: metricsProvider,
	}
	d.metricsProvider.GetProducerDeciderMetrics(d.options.groundTopic, topic, d.options.msgGoto.String()).DecidersOpened.Inc()
	return d, nil
}

func (d *producerStatusDecider) Decide(ctx context.Context, msg *pulsar.ProducerMessage,
	checkStatus checker.CheckStatus) (mid pulsar.MessageID, err error, decided bool) {
	// valid check status
	if !checkStatus.IsPassed() {
		err = errors.New(fmt.Sprintf("%v decider failed to execute as check status is not passed", d.options.msgStatus))
		decided = false
		return
	}

	// consume time info
	message.Helper.InjectConsumeTime(&msg.Properties, checkStatus.GetGotoExtra().ConsumeTime)

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
	d.router.Chan() <- &RouteMessage{
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

func (d *producerStatusDecider) DecideAsync(ctx context.Context, msg *pulsar.ProducerMessage, checkStatus checker.CheckStatus,
	callback func(pulsar.MessageID, *pulsar.ProducerMessage, error)) (decided bool) {
	// valid check status
	if !checkStatus.IsPassed() {
		err := errors.New(fmt.Sprintf("%v decider failed to execute as check status is not passed", decider.GotoDead))
		callback(nil, msg, err)
		decided = false
		return
	}

	// consume time info
	message.Helper.InjectConsumeTime(&msg.Properties, checkStatus.GetGotoExtra().ConsumeTime)

	// send
	d.router.Chan() <- &RouteMessage{
		producerMsg: msg,
		callback:    callback,
	}

	return true
}

func (d *producerStatusDecider) close() {
	topic := d.options.groundTopic + d.options.level.TopicSuffix() + d.options.msgStatus.TopicSuffix()
	d.metricsProvider.GetProducerDeciderMetrics(d.options.groundTopic, topic, d.options.msgGoto.String()).DecidersOpened.Desc()
}
