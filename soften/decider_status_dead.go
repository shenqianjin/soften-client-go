package soften

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/handler"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

type deadDecideOptions struct {
	topic string // default ${TOPIC}_RETRYING, 固定后缀，不允许定制
	//enable bool   // 内部判断使用
}

type deadDecider struct {
	router  *reRouter
	options deadDecideOptions
	metrics *internal.ListenerDecideGotoMetrics
}

func newDeadDecider(client *client, listener *consumeListener, options deadDecideOptions) (*deadDecider, error) {
	rt, err := newReRouter(client.logger, client.Client, reRouterOptions{Topic: options.topic})
	if err != nil {
		return nil, err
	}
	metrics := client.metricsProvider.GetListenerDecideGotoMetrics(listener.logTopics, listener.logLevels, handler.GotoDead)
	decider := &deadDecider{router: rt, options: options, metrics: metrics}
	metrics.DecidersOpened.Inc()
	return decider, nil
}

func (d *deadDecider) Decide(msg pulsar.ConsumerMessage, cheStatus checker.CheckStatus) bool {
	if !cheStatus.IsPassed() {
		return false
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
	// record previous level/status information
	message.Helper.InjectPreviousLevel(msg, &props)
	message.Helper.InjectPreviousStatus(msg, &props)

	producerMsg := pulsar.ProducerMessage{
		Payload:     msg.Payload(),
		Key:         msg.Key(),
		OrderingKey: msg.OrderingKey(),
		Properties:  props,
		EventTime:   msg.EventTime(),
	}
	d.router.Chan() <- &RerouteMessage{
		consumerMsg: msg,
		producerMsg: producerMsg,
	}
	return true
}

func (d *deadDecider) close() {
	d.metrics.DecidersOpened.Dec()
}
