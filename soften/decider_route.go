package soften

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/handler"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

type routeDeciderOptions struct {
	connectInSyncEnable bool // Optional: 是否同步建立连接, 首次发送消息需阻塞等待客户端与服务端连接完成

	// extra for upgrade/degrade

	upgradeLevel internal.TopicLevel
	degradeLevel internal.TopicLevel
}

type routeDecider struct {
	logger      log.Logger
	client      pulsar.Client
	topic       string
	handleGoto  internal.HandleGoto
	options     *routeDeciderOptions
	routers     map[string]*router
	routersLock sync.RWMutex
	metrics     *internal.DecideGotoMetrics
}

func newRouteDecider(producer *producer, handleGoto internal.HandleGoto, options *routeDeciderOptions) (*routeDecider, error) {
	if handleGoto == handler.GotoUpgrade {
		if options.upgradeLevel == "" {
			return nil, errors.New(fmt.Sprintf("upgrade level is missing for %v router", handleGoto))
		}
	}
	if handleGoto == handler.GotoDegrade {
		if options.degradeLevel == "" {
			return nil, errors.New(fmt.Sprintf("degrade level is missing for %v router", handleGoto))
		}
	}
	metrics := producer.client.metricsProvider.GetProducerDecideGotoMetrics(producer.topic, handleGoto.String())
	rtrDecider := &routeDecider{
		logger:     producer.logger,
		client:     producer.client.Client,
		topic:      producer.topic,
		handleGoto: handleGoto,
		options:    options,
		routers:    make(map[string]*router),
		metrics:    metrics,
	}
	metrics.DecidersOpened.Inc()
	return rtrDecider, nil
}

func (d *routeDecider) Decide(ctx context.Context, msg *pulsar.ProducerMessage,
	checkStatus checker.CheckStatus) (mid pulsar.MessageID, err error, decided bool) {
	if !checkStatus.IsPassed() {
		return nil, nil, false
	}
	// process discard
	if d.handleGoto == handler.GotoDiscard {
		d.logger.Warnf(fmt.Sprintf("discard message. payload size: %v, properties: %v", len(msg.Payload), msg.Properties))
		decided = true
		return
	}
	// parse topic
	routeTopic := d.parseRouteTopic(checkStatus)
	// get or create router
	rtr, err := d.internalSafeGetRouter(routeTopic)
	if err != nil {
		d.logger.Warnf("failed to create router for topic: %s", routeTopic)
		return nil, err, false
	}
	if !rtr.ready {
		// happens in connect in async mode: skip to decide
		// back to other router or main topic before the checked router is ready
		d.logger.Warnf("router is still not ready for topic: %s", routeTopic)
		return nil, nil, false
	}
	// send
	mid, err = rtr.Send(ctx, msg)
	if err != nil {
		d.logger.Warnf("failed to route message, payload size: %v, properties: %v", len(msg.Payload), msg.Properties)
		return mid, err, false
	}
	return mid, err, true
}

func (d *routeDecider) DecideAsync(ctx context.Context, msg *pulsar.ProducerMessage, checkStatus checker.CheckStatus,
	callback func(pulsar.MessageID, *pulsar.ProducerMessage, error)) (decided bool) {
	if !checkStatus.IsPassed() {
		callback(nil, msg, errors.New(fmt.Sprintf("%v decider failed to execute as check status is not passed", d.handleGoto)))
		decided = false
		return
	}
	// process discard
	if d.handleGoto == handler.GotoDiscard {
		d.logger.Warnf(fmt.Sprintf("discard message. payload size: %v, properties: %v", len(msg.Payload), msg.Properties))
		callback(nil, msg, nil)
		decided = true
		return
	}
	// parse topic
	routeTopic := d.parseRouteTopic(checkStatus)
	// get or create router
	rtr, err := d.internalSafeGetRouter(routeTopic)
	if err != nil {
		d.logger.Warnf("failed to create router for topic: %s", routeTopic)
		return false
	}
	if !rtr.ready {
		// wait router until it's ready
		if d.options.connectInSyncEnable {
			<-rtr.readyCh
		} else {
			// back to other router or main topic before the checked router is ready
			d.logger.Warnf("router is still not ready for topic: %s", routeTopic)
			return false
		}
	}
	// send
	rtr.SendAsync(ctx, msg, callback)

	return true

}

func (d *routeDecider) parseRouteTopic(checkStatus checker.CheckStatus) string {
	routeTopic := ""
	switch d.handleGoto {
	case handler.GotoDead:
		routeTopic = d.topic + message.StatusDead.TopicSuffix()
	case handler.GotoUpgrade:
		routeTopic = d.topic + d.options.upgradeLevel.TopicSuffix()
	case handler.GotoDegrade:
		routeTopic = d.topic + d.options.degradeLevel.TopicSuffix()
	case handler.GotoBlocking:
		routeTopic = d.topic + message.StatusBlocking.TopicSuffix()
	case handler.GotoPending:
		routeTopic = d.topic + message.StatusPending.TopicSuffix()
	case handler.GotoRetrying:
		routeTopic = d.topic + message.StatusRetrying.TopicSuffix()
	case internalGotoRoute:
		routeTopic = checkStatus.GetRouteTopic()
	}
	return routeTopic
}

func (d *routeDecider) internalSafeGetRouter(topic string) (*router, error) {
	d.routersLock.RLock()
	rtr, ok := d.routers[topic]
	d.routersLock.RUnlock()
	if ok {
		return rtr, nil
	}
	rtOption := routerOptions{Topic: topic, connectInSyncEnable: d.options.connectInSyncEnable}
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
		return rtr, nil
	}
}

func (d *routeDecider) close() {
	d.metrics.DecidersOpened.Dec()
}
