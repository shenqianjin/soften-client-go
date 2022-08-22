package soften

import (
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

type rerouteDecider struct {
	client      *client
	logger      log.Logger
	routers     map[string]*reRouter
	routersLock sync.RWMutex
	policy      *config.ReroutePolicy
	metrics     *internal.ListenerDecideGotoMetrics
}

func newRerouteDecider(client *client, listener *consumeListener, policy *config.ReroutePolicy) (*rerouteDecider, error) {
	routers := make(map[string]*reRouter)
	metrics := client.metricsProvider.GetListenerDecideGotoMetrics(listener.logTopics, listener.logLevels, internalGotoReroute)
	rtrHandler := &rerouteDecider{client: client, logger: client.logger, routers: routers, policy: policy, metrics: metrics}
	metrics.DecidersOpened.Inc()
	return rtrHandler, nil
}

func (d *rerouteDecider) Decide(msg pulsar.ConsumerMessage, cheStatus checker.CheckStatus) bool {
	if !cheStatus.IsPassed() || cheStatus.GetRouteTopic() == "" {
		return false
	}
	rtr, err := d.internalSafeGetReRouterInAsync(cheStatus.GetRouteTopic())
	if err != nil {
		return false
	}
	if !rtr.ready {
		if d.policy.ConnectInSyncEnable {
			<-rtr.readyCh
		} else {
			return false
		}
	}
	// prepare to reroute
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
	rtr.Chan() <- &RerouteMessage{
		consumerMsg: msg,
		producerMsg: producerMsg,
	}
	return true
}

func (d *rerouteDecider) internalSafeGetReRouterInAsync(topic string) (*reRouter, error) {
	d.routersLock.RLock()
	rtr, ok := d.routers[topic]
	d.routersLock.RUnlock()
	if ok {
		return rtr, nil
	}
	rtOption := reRouterOptions{Topic: topic, connectInSyncEnable: d.policy.ConnectInSyncEnable}
	d.routersLock.Lock()
	defer d.routersLock.Unlock()
	rtr, ok = d.routers[topic]
	if ok {
		return rtr, nil
	}
	if newRtr, err := newReRouter(d.logger, d.client.Client, rtOption); err != nil {
		return nil, err
	} else {
		rtr = newRtr
		d.routers[topic] = newRtr
		return rtr, nil
	}
}

func (d *rerouteDecider) close() {
	d.metrics.DecidersOpened.Dec()
}
