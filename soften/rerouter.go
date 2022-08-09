package soften

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/internal/backoff"
)

// ------ base reRouter ------

type reRouterOptions struct {
	Topic               string
	connectInSyncEnable bool
	//connectMaxRetries   uint
	//MaxDeliveries uint
}

type reRouter struct {
	client    pulsar.Client
	producer  pulsar.Producer
	option    reRouterOptions
	messageCh chan *RerouteMessage
	closeCh   chan interface{}
	readyCh   chan struct{}
	logger    log.Logger
	ready     bool
	initOnce  sync.Once
}

func newReRouter(logger log.Logger, client pulsar.Client, options reRouterOptions) (*reRouter, error) {
	r := &reRouter{
		client:  client,
		option:  options,
		logger:  logger.SubLogger(log.Fields{"reroute-topic": options.Topic}),
		readyCh: make(chan struct{}, 1),
	}

	/*if options.connectMaxRetries <= 0 {
		return nil, errors.New("reRouterOptions.connectMaxRetries needs to be > 0")
	}*/

	if options.Topic == "" {
		return nil, errors.New("reRouterOptions.Topic needs to be set to a valid topic name")
	}

	r.messageCh = make(chan *RerouteMessage, 1)
	r.closeCh = make(chan interface{}, 1)
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

func (r *reRouter) Chan() chan *RerouteMessage {
	return r.messageCh
}

func (r *reRouter) run() {
	// wait until it is ready
	<-r.readyCh
	for {
		select {
		case rm := <-r.messageCh:
			//r.logger.Infof("reroute ********************************* %d", count)
			//count++
			r.logger.WithField("msgID", rm.consumerMsg.ID()).Debugf("Got message for topic: %s", r.option.Topic)

			msgID := rm.consumerMsg.ID()
			r.producer.SendAsync(context.Background(), &rm.producerMsg, func(messageID pulsar.MessageID,
				producerMessage *pulsar.ProducerMessage, err error) {
				if err != nil {
					r.logger.WithError(err).WithField("msgID", msgID).Errorf("Failed to send message to topic: %s", r.option.Topic)
					rm.consumerMsg.Consumer.Nack(rm.consumerMsg)
				} else {
					r.logger.WithField("msgID", msgID).Debugf("Succeed to send message to topic: %s", r.option.Topic)
					rm.consumerMsg.Consumer.AckID(msgID)
				}
			})

		case <-r.closeCh:
			if r.producer != nil {
				r.producer.Close()
			}
			r.logger.Debugf("Closed reRouter for topic: %s", r.option.Topic)
			return
		}
	}
}

func (r *reRouter) close() {
	// Attempt to write on the close channel, without blocking
	select {
	case r.closeCh <- nil:
	default:
	}
	close(r.readyCh)
}

func (r *reRouter) initializeProducer() {
	r.initOnce.Do(func() {
		// Retry to create producer indefinitely
		backoffPolicy := &backoff.Backoff{}
		for {
			producer, err := r.client.CreateProducer(pulsar.ProducerOptions{
				Topic:                   r.option.Topic,
				CompressionType:         pulsar.LZ4,
				BatchingMaxPublishDelay: 100 * time.Millisecond,
			})

			if err != nil {
				r.logger.WithError(err).Errorf("Failed to create producer for topic: %s", r.option.Topic)
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
