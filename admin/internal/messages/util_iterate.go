package messages

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/antonmedv/expr/vm"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/sirupsen/logrus"
)

type iterateOptions struct {
	// resource information
	brokerUrl string
	webUrl    string
	topic     string

	// conditions
	conditions       []*vm.Program
	startPublishTime time.Time
	startEventTime   time.Time

	// others
	printProgressIterateInterval uint64
}

type iterateConsumerOptions struct {
	subscription string
	// end conditions
	iterateTimeout uint32
	matchTimeout   uint32
	endPublishTime time.Time
	endEventTime   time.Time
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

type iterateResults []iterateResult

func (irs *iterateResults) PrettyString() string {
	if len(*irs) == 0 {
		return "empty iterate result"
	}
	if len(*irs) > 1 {
		total := iterateResult{}
		var iterateString string
		for index, it := range *irs {
			total.iterated += it.iterated
			total.matched += it.matched
			total.handled += it.handled
			iterateString += fmt.Sprintf("partitioned-%d >>> %v\n", index, it.PrettyString())
		}
		return iterateString + fmt.Sprintf("total >>> %s", total.PrettyString())
	}
	return (*irs)[0].PrettyString()
}

func iterateInternalByReader(options iterateOptions,
	handleFunc func(msg pulsar.Message) bool) iterateResults {
	topics := make([]string, 0)
	// calculate non-partitioned topics
	// pulsar-go-client doesn't support reader on partitioned topics currently.
	// @see: https://github.com/apache/pulsar-client-go/issues/553
	nonPartitionedManager := admin.NewNonPartitionedTopicManager(options.webUrl)
	_, err := nonPartitionedManager.Stats(options.topic)
	if err == nil {
		topics = append(topics, options.topic)
	} else if strings.Contains(err.Error(), admin.Err404NotFound) {
		partitionedManager := admin.NewPartitionedTopicManager(options.webUrl)
		stat2, err2 := partitionedManager.Stats(options.topic)
		if err2 != nil || stat2.Metadata.Partitions <= 0 {
			logrus.Fatal(err)
		}
		for i := 0; i < stat2.Metadata.Partitions; i++ {
			topics = append(topics, options.topic+"-partition-"+strconv.Itoa(i))
		}
	} else {
		logrus.Fatal(err)
	}
	// src client
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: options.brokerUrl})
	if err != nil {
		logrus.Fatal(err)
	}
	defer client.Close()

	// iterate
	results := make(iterateResults, 0)
	for _, topic := range topics {
		iterated := atomic.Uint64{}
		hit := atomic.Uint64{}
		handled := atomic.Uint64{}
		var firstMatchedMsg pulsar.Message
		var lastMatchedMsg pulsar.Message
		var firstHandledMsg pulsar.Message
		var lastHandledMsg pulsar.Message
		// src reader
		reader, err := client.CreateReader(pulsar.ReaderOptions{
			Topic:          topic,
			StartMessageID: pulsar.EarliestMessageID(),
		})
		if err != nil {
			logrus.Fatal(err)
		}
		defer reader.Close()

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
		r := iterateResult{iterated: iterated.Load(), matched: hit.Load(), handled: handled.Load(),
			firstMatchedMsg: firstMatchedMsg, lastMatchedMsg: lastMatchedMsg,
			firstHandledMsg: firstMatchedMsg, lastHandledMsg: lastHandledMsg}
		results = append(results, r)
	}
	return results
}

type consumerIterateResult struct {
	iterateResult

	unmatchedHandled uint64

	unmatchedFirstHandledMsg pulsar.Message
	unmatchedLastHandledMsg  pulsar.Message
}

func (ir *consumerIterateResult) PrettyString() string {
	var unmatchedHandleString string
	if ir.unmatchedFirstHandledMsg != nil {
		unmatchedHandleString = fmt.Sprintf("unmatched-handled: %v [%v - %v]", ir.unmatchedHandled, ir.unmatchedFirstHandledMsg.ID(), ir.unmatchedLastHandledMsg.ID())
	} else {
		unmatchedHandleString = fmt.Sprintf("unmatched-handled: %v", ir.unmatchedHandled)
	}
	return fmt.Sprintf("%s, %s", ir.iterateResult.PrettyString(), unmatchedHandleString)
}

func iterateInternalByConsumer(options iterateOptions, consumerOptions iterateConsumerOptions,
	handleFunc func(msg pulsar.ConsumerMessage, matched bool) bool) consumerIterateResult {
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
	unmatchedHandled := atomic.Uint64{}
	var firstMatchedMsg pulsar.Message
	var lastMatchedMsg pulsar.Message
	var firstHandledMsg pulsar.Message
	var lastHandledMsg pulsar.Message
	var unmatchedFirstHandledMsg pulsar.Message
	var unmatchedLastHandledMsg pulsar.Message
	lastIterateTime := time.Time{}
	lastMatchTime := time.Time{}
	ticker := time.Tick(time.Second)
	iterateDone := false
	for {
		select {
		case msg := <-consumer.Chan():
			logrus.Debugf("started to iterate src mid: %v", msg.ID())
			// check end
			if !consumerOptions.endPublishTime.IsZero() {
				if !msg.PublishTime().Before(consumerOptions.endPublishTime) {
					logrus.Debugf("break as detected msg publish time is equal or after the end publish time option")
					iterateDone = true
					break
				}
			}
			if !consumerOptions.endEventTime.IsZero() && !msg.EventTime().IsZero() {
				if !msg.EventTime().Before(consumerOptions.endEventTime) {
					logrus.Debugf("break as detected msg event time is equal or after the end event time option")
					iterateDone = true
					break
				}
			}
			// iterator
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
				if handleFunc(msg, false) {
					unmatchedHandled.Add(1)
					if unmatchedFirstHandledMsg == nil {
						unmatchedFirstHandledMsg = msg
					}
					unmatchedLastHandledMsg = msg
				}
				continue
			}
			hit.Add(1)
			if firstMatchedMsg == nil {
				firstMatchedMsg = msg
			}
			lastMatchedMsg = msg
			lastMatchTime = time.Now()

			// handle
			if handleFunc(msg, true) {
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
				iterateDone = true
				break
			}
			if consumerOptions.matchTimeout > 0 && time.Now().Sub(lastMatchTime).Seconds() > float64(consumerOptions.matchTimeout) {
				logrus.Debugf("break as it it over matched timeout since last matched")
				iterateDone = true
				break
			}
		}
		if iterateDone {
			break
		}
	}
	return consumerIterateResult{
		iterateResult: iterateResult{iterated: iterated.Load(), matched: hit.Load(), handled: handled.Load(),
			lastMatchedMsg: lastMatchedMsg, lastHandledMsg: lastHandledMsg},
		unmatchedHandled:         unmatchedHandled.Load(),
		unmatchedFirstHandledMsg: unmatchedFirstHandledMsg,
		unmatchedLastHandledMsg:  unmatchedLastHandledMsg,
	}
}
