package soften

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/internal/backoff"
)

// ------ router ------

type routerOptions struct {
	Topic               string
	connectInSyncEnable bool

	BackoffMaxTimes uint
	BackoffDelays   []string
	BackoffPolicy   config.BackoffPolicy
}

type router struct {
	client    pulsar.Client
	producer  pulsar.Producer
	options   routerOptions
	logger    log.Logger
	messageCh chan *RouteMessage
	initOnce  sync.Once
	readyCh   chan struct{}
	ready     bool
	closeCh   chan interface{}
}

func newRouter(logger log.Logger, client pulsar.Client, options routerOptions) (*router, error) {
	if options.Topic == "" {
		return nil, errors.New("routerOptions.WithTopic needs to be set to a valid topic name")
	}
	r := &router{
		client:    client,
		options:   options,
		logger:    logger.SubLogger(log.Fields{"transfer-topic": options.Topic}),
		messageCh: make(chan *RouteMessage, 1),
		readyCh:   make(chan struct{}, 1),
		closeCh:   make(chan interface{}, 1),
	}
	// create real producer
	if options.connectInSyncEnable {
		// sync create
		r.initializeProducer()
	} else {
		// async create
		go r.initializeProducer()
	}
	go r.run()
	return r, nil
}

func (r *router) Chan() chan *RouteMessage {
	return r.messageCh
}

func (r *router) run() {
	for {
		select {
		case rm := <-r.messageCh:
			ctx := context.Background()
			r.producer.SendAsync(ctx, rm.producerMsg, func(messageID pulsar.MessageID,
				producerMessage *pulsar.ProducerMessage, err error) {
				for i := uint(0); err != nil && i < r.options.BackoffMaxTimes; i++ {
					_, err = r.producer.Send(ctx, rm.producerMsg)
				}
				r.logger.WithField("msgID", messageID).Debugf("routed message for topic: %s", r.options.Topic)
				rm.callback(messageID, producerMessage, err)
			})

		case <-r.closeCh:
			if r.producer != nil {
				r.producer.Close()
			}
			r.logger.Debugf("Closed router for topic: %s", r.options.Topic)
			return
		}
	}
}

func (r *router) initializeProducer() {
	r.initOnce.Do(func() {
		// Retry to create producer indefinitely
		backoffPolicy := &backoff.Backoff{}
		for {
			producer, err := r.client.CreateProducer(pulsar.ProducerOptions{
				Topic:                   r.options.Topic,
				CompressionType:         pulsar.LZ4,
				BatchingMaxPublishDelay: 100 * time.Millisecond,
			})

			if err != nil {
				r.logger.WithError(err).Errorf("Failed to create producer for topic: %s", r.options.Topic)
				time.Sleep(backoffPolicy.Next())
				continue
			} else {
				r.producer = producer
				r.ready = true
				r.readyCh <- struct{}{}
				return
			}
		}
	})
}

func (r *router) close() {
	// Attempt to write on the close channel, without blocking
	select {
	case r.closeCh <- nil:
	default:
	}
	close(r.readyCh)
}
