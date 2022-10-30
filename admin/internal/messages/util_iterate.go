package messages

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/antonmedv/expr/vm"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
)

type iterateOptions struct {
	// resource information
	brokerUrl string
	topic     string

	// conditions
	conditions       []*vm.Program
	startPublishTime time.Time
	startEventTime   time.Time

	// others
	printProgressIterateInterval uint64
}

type iterateConsumerOptions struct {
	subscription   string
	iterateTimeout uint32
	matchTimeout   uint32
}

type iterateResult struct {
	iterated uint64
	matched  uint64
	handled  uint64

	firstMatchedMsg pulsar.Message
	lastMatchedMsg  pulsar.Message
	firstHandledMsg pulsar.Message
	lastHandledMsg  pulsar.Message
}

func (ir *iterateResult) PrettyString() string {
	iterateString := fmt.Sprintf("iterated: %v", ir.iterated)
	var matchString string
	if ir.firstMatchedMsg != nil {
		matchString = fmt.Sprintf("matched: %v [%v - %v]", ir.matched, ir.firstMatchedMsg.ID(), ir.lastMatchedMsg.ID())
	} else {
		matchString = fmt.Sprintf("matched: %v", ir.matched)
	}
	var handleString string
	if ir.firstHandledMsg != nil {
		handleString = fmt.Sprintf("handled: %v [%v - %v]", ir.handled, ir.firstHandledMsg.ID(), ir.lastHandledMsg.ID())
	} else {
		handleString = fmt.Sprintf("handled: %v", ir.matched)
	}
	return fmt.Sprintf("%s, %s, %s", iterateString, matchString, handleString)
}

func iterateInternalByReader(options iterateOptions,
	handleFunc func(msg pulsar.Message) bool) iterateResult {
	// src client
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: options.brokerUrl})
	if err != nil {
		logrus.Fatal(err)
	}
	defer client.Close()

	// src reader
	reader, err := client.CreateReader(pulsar.ReaderOptions{
		Topic:          options.topic,
		StartMessageID: pulsar.EarliestMessageID(),
	})
	if err != nil {
		logrus.Fatal(err)
	}
	defer reader.Close()

	// iterate
	iterated := atomic.Uint64{}
	hit := atomic.Uint64{}
	handled := atomic.Uint64{}
	var firstMatchedMsg pulsar.Message
	var lastMatchedMsg pulsar.Message
	var firstHandledMsg pulsar.Message
	var lastHandledMsg pulsar.Message
	for reader.HasNext() {
		msg, err := reader.Next(context.Background())
		if err != nil {
			logrus.Fatal(err)
		}
		logrus.Debugf("started to iterate src mid: %v", msg.ID())
		iterated.Add(1)
		iteratedVal := iterated.Load()
		// info progress
		if options.printProgressIterateInterval > 0 && iteratedVal%options.printProgressIterateInterval == 0 {
			logrus.Infof("iterate progress => iterated: %v, matched: %v, handled: %v. next => mid: %v, publish time: %v, event time: %v\n",
				iteratedVal, hit.Load(), handled.Load(), msg.ID(), msg.PublishTime(), msg.EventTime())
		}
		// skip unmatched messages
		if !matched(msg, matchOptions{
			conditions:       options.conditions,
			startEventTime:   options.startEventTime,
			startPublishTime: options.startPublishTime}) {
			continue
		}
		hit.Add(1)
		if firstMatchedMsg == nil {
			firstMatchedMsg = msg
		}
		lastMatchedMsg = msg

		// handle
		if handleFunc(msg) {
			handled.Add(1)
			if firstHandledMsg == nil {
				firstHandledMsg = msg
			}
			lastHandledMsg = msg
		}
		logrus.Debugf("ended to iterate src mid: %v", msg.ID())
	}
	return iterateResult{iterated: iterated.Load(), matched: hit.Load(), handled: handled.Load(),
		firstMatchedMsg: firstMatchedMsg, lastMatchedMsg: lastMatchedMsg,
		firstHandledMsg: firstMatchedMsg, lastHandledMsg: lastHandledMsg}
}

func iterateInternalByConsumer(options iterateOptions, consumerOptions iterateConsumerOptions,
	stopChan <-chan struct{}, handleFunc func(msg pulsar.ConsumerMessage) bool) iterateResult {
	// src client
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: options.brokerUrl})
	if err != nil {
		logrus.Fatal(err)
	}
	defer client.Close()

	// src consumer
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       options.topic,
		SubscriptionName:            consumerOptions.subscription,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		Type:                        pulsar.Shared,
	})
	if err != nil {
		logrus.Fatal(err)
	}
	defer consumer.Close()

	// iterate
	iterated := atomic.Uint64{}
	hit := atomic.Uint64{}
	handled := atomic.Uint64{}
	var firstMatchedMsg pulsar.Message
	var lastMatchedMsg pulsar.Message
	var firstHandledMsg pulsar.Message
	var lastHandledMsg pulsar.Message
	lastIterateTime := time.Time{}
	lastMatchTime := time.Time{}
	ticker := time.Tick(time.Second)
	for {
		select {
		case msg := <-consumer.Chan():
			logrus.Debugf("started to iterate src mid: %v", msg.ID())
			iterated.Add(1)
			iteratedVal := iterated.Load()
			lastIterateTime = time.Now()
			// info progress
			if options.printProgressIterateInterval > 0 && iteratedVal%options.printProgressIterateInterval == 0 {
				logrus.Infof("iterate progress => iterated: %v, matched: %v, handled: %v. next => mid: %v, publish time: %v, event time: %v\n",
					iteratedVal, hit.Load(), handled.Load(), msg.ID(), msg.PublishTime(), msg.EventTime())
			}
			// skip unmatched messages
			if !matched(msg, matchOptions{
				conditions:       options.conditions,
				startEventTime:   options.startEventTime,
				startPublishTime: options.startPublishTime}) {
				continue
			}
			hit.Add(1)
			if firstMatchedMsg == nil {
				firstMatchedMsg = msg
			}
			lastMatchedMsg = msg
			lastMatchTime = time.Now()

			// handle
			if handleFunc(msg) {
				handled.Add(1)
				if firstHandledMsg == nil {
					firstHandledMsg = msg
				}
				lastHandledMsg = msg
			}
			logrus.Debugf("ended to iterate src mid: %v", msg.ID())
		case <-ticker:
			if consumerOptions.iterateTimeout > 0 && time.Now().Sub(lastIterateTime).Seconds() > float64(consumerOptions.iterateTimeout) {
				logrus.Debugf("break as it it over iterate timeout since last iteartion")
				break
			}
			if consumerOptions.matchTimeout > 0 && time.Now().Sub(lastMatchTime).Seconds() > float64(consumerOptions.matchTimeout) {
				logrus.Debugf("break as it it over matched timeout since last matched")
				break
			}
		case <-stopChan:
			break
		}
	}
	return iterateResult{iterated: iterated.Load(), matched: hit.Load(), handled: handled.Load(),
		lastMatchedMsg: lastMatchedMsg, lastHandledMsg: lastHandledMsg}
}
