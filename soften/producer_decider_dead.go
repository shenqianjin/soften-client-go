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

type producerDeadDeciderOptions struct {
	groundTopic string
	level       internal.TopicLevel // only used for metrics, dead status is not level related
}

type producerDeadDecider struct {
	router          *router
	logger          log.Logger
	options         *producerDeadDeciderOptions
	metricsProvider *internal.MetricsProvider
}

func newProduceDeadDecider(producer *producer, options *producerDeadDeciderOptions, metricsProvider *internal.MetricsProvider) (*producerDeadDecider, error) {
	if options == nil {
		return nil, errors.New("missing options for produce dead decider")
	}
	if options.groundTopic == "" {
		return nil, errors.New("topic is blank")
	}
	rtOptions := routerOptions{
		Topic:               options.groundTopic + message.StatusDead.TopicSuffix(),
		connectInSyncEnable: true,
	}
	rt, err := newRouter(producer.logger, producer.client.Client, rtOptions)
	if err != nil {
		return nil, err
	}
	d := &producerDeadDecider{router: rt, logger: producer.logger, options: options, metricsProvider: metricsProvider}
	topic := options.groundTopic + options.level.TopicSuffix()
	d.metricsProvider.GetProducerDeciderMetrics(d.options.groundTopic, topic, decider.GotoDead.String()).DecidersOpened.Inc()
	return d, nil
}

func (d *producerDeadDecider) Decide(ctx context.Context, msg *pulsar.ProducerMessage,
	checkStatus checker.CheckStatus) (mid pulsar.MessageID, err error, decided bool) {
	// valid check status
	if !checkStatus.IsPassed() {
		err = errors.New(fmt.Sprintf("%v decider failed to execute as check status is not passed", decider.GotoDead))
		decided = false
		return
	}

	// consume time info
	message.Helper.InjectConsumeTime(msg.Properties, checkStatus.GetGotoExtra().ConsumeTime)

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

func (d *producerDeadDecider) DecideAsync(ctx context.Context, msg *pulsar.ProducerMessage, checkStatus checker.CheckStatus,
	callback func(pulsar.MessageID, *pulsar.ProducerMessage, error)) (decided bool) {
	// valid check status
	if !checkStatus.IsPassed() {
		err := errors.New(fmt.Sprintf("%v decider failed to execute as check status is not passed", decider.GotoDead))
		callback(nil, msg, err)
		decided = false
		return
	}

	// consume time info
	message.Helper.InjectConsumeTime(msg.Properties, checkStatus.GetGotoExtra().ConsumeTime)

	// send
	d.router.Chan() <- &RouteMessage{
		producerMsg: msg,
		callback:    callback,
	}

	return true
}

func (d *producerDeadDecider) close() {
	topic := d.options.groundTopic + d.options.level.TopicSuffix()
	d.metricsProvider.GetProducerDeciderMetrics(d.options.groundTopic, topic, decider.GotoDead.String()).DecidersOpened.Desc()
}
