package soften

import (
	"errors"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/internal/backoff"
)

// ------ router ------

type routerOptions struct {
	Topic               string
	connectInSyncEnable bool
}

type router struct {
	pulsar.Producer
	client   pulsar.Client
	options  routerOptions
	logger   log.Logger
	initOnce sync.Once
	ready    bool
	readyCh  chan struct{}
}

func newRouter(logger log.Logger, client pulsar.Client, options routerOptions) (*router, error) {
	if options.Topic == "" {
		return nil, errors.New("routerOptions.Topic needs to be set to a valid topic name")
	}
	r := &router{
		client:  client,
		options: options,
		logger:  logger.SubLogger(log.Fields{"route-topic": options.Topic}),
		readyCh: make(chan struct{}, 1),
	}
	// create real producer
	if options.connectInSyncEnable {
		// sync create
		r.initializeProducer()
	} else {
		// async create
		go r.initializeProducer()
	}
	return r, nil
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
				r.ready = true
				r.readyCh <- struct{}{}
				r.Producer = producer
				break
			}
		}
	})

}
