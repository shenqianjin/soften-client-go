package messages

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/util"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type tidyArgs struct {
	matchedTo          string // 匹配消息转发至
	unmatchedTo        string // 不匹配消息转发至
	matchedAsDiscard   bool   // 丢弃匹配消息
	unmatchedAsDiscard bool   // 丢弃不匹配消息

	printProgressIterateInterval uint64 // 每遍历多少个消息打印进度
	subscription                 string //
	iterateTimeout               uint32 // 终止条件: 能遍历到消息的超时时间
	matchTimeout                 uint32 // 终止条件: 有能匹配消息的超时时间

	publishBatchEnable bool
	publishMaxTimes    uint64
}

func newTidyCommand(rtArgs *internal.RootArgs, mdlArgs *messagesArgs) *cobra.Command {
	cmdArgs := &tidyArgs{}
	cmd := &cobra.Command{
		Use:   "tidy ",
		Short: "tidy messages of an topic, matched messages can be published to any topic or discarded, so does unmatched ones",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			mdlArgs.topic = args[0]
			tidyMessages(rtArgs, mdlArgs, cmdArgs)
		},
	}
	// parse variables for source
	cmd.Flags().Uint64Var(&cmdArgs.printProgressIterateInterval, "print-progress-iterate-interval", 10000, util.PrintProgressIterateIntervalUsage)
	cmd.Flags().StringVarP(&cmdArgs.subscription, "subscription", "s", "", util.SingleSubscriptionUsage)
	cmd.Flags().Uint32Var(&cmdArgs.iterateTimeout, "iterate-timeout", 0, "iterate timeout")
	cmd.Flags().Uint32Var(&cmdArgs.matchTimeout, "match-timeout", 0, "match timeout")
	// parse variables for destination
	cmd.Flags().StringVar(&cmdArgs.matchedTo, "matched-to", "", "topic to publish these matched messages to")
	cmd.Flags().BoolVar(&cmdArgs.matchedAsDiscard, "matched-as-discard", false, "whether discards these matched messages, it is ignored if '--matched-to' is not empty")
	cmd.Flags().BoolVar(&cmdArgs.unmatchedAsDiscard, "unmatched-as-discard", false, "whether discards these unmatched messages, it is ignored if '--unmatched-to' is not empty")
	cmd.Flags().BoolVarP(&cmdArgs.publishBatchEnable, "publish-batch-enable", "b", false, util.BatchEnableUsage)
	cmd.Flags().Uint64Var(&cmdArgs.publishMaxTimes, "publish-max-times", 0, util.PublishMaxTimesUsage)

	return cmd
}

func tidyMessages(rtArgs *internal.RootArgs, mdlArgs *messagesArgs, cmdArgs *tidyArgs) {
	// parse vars
	parsedMdlVars := parseAndValidateMessagesVars(rtArgs, mdlArgs)
	// valid subs
	if cmdArgs.subscription == "" {
		logrus.Fatalf("missing subscription option. please specify one with --subscription or -s\n")
	} else if strings.Contains(cmdArgs.subscription, ",") {
		logrus.Fatalf("multiple subscription names are not supported for this command\n")
	}
	// check dest topics
	if cmdArgs.matchedTo != "" {
		manager := admin.NewRobustTopicManager(rtArgs.Url)
		if _, err := manager.Stats(cmdArgs.matchedTo); err != nil {
			logrus.Fatalf("invalid 'matched-to' destination topic: %v, err: %v\n", cmdArgs.matchedTo, err)
		}
	} else if !cmdArgs.matchedAsDiscard {
		logrus.Fatalf("missing 'matched-to' destination. please specify one by '--matched-to' or enable '--matched-as-discard'")
	}
	if cmdArgs.unmatchedTo != "" {
		manager := admin.NewRobustTopicManager(rtArgs.Url)
		if _, err := manager.Stats(cmdArgs.unmatchedTo); err != nil {
			logrus.Fatalf("invalid 'unmatched-to' destination topic: %v, err: %v\n", cmdArgs.unmatchedTo, err)
		}
	} else if !cmdArgs.unmatchedAsDiscard {
		logrus.Fatalf("missing 'unmatched-to' destination. please specify one by '--unmatched-to' or enable '--unmatched-as-discard'")
	}
	// dest client
	destClient, err := pulsar.NewClient(pulsar.ClientOptions{URL: mdlArgs.BrokerUrl})
	if err != nil {
		logrus.Fatal(err)
	}
	defer destClient.Close()
	// desc producer
	var matchedProducer, unmatchedProducer pulsar.Producer
	if cmdArgs.matchedTo != "" {
		matchedProducer, err = destClient.CreateProducer(pulsar.ProducerOptions{Topic: cmdArgs.matchedTo})
		if err != nil {
			logrus.Fatal(err)
		}
		defer matchedProducer.Close()
	}
	if cmdArgs.unmatchedTo != "" {
		unmatchedProducer, err = destClient.CreateProducer(pulsar.ProducerOptions{Topic: cmdArgs.unmatchedTo})
		if err != nil {
			logrus.Fatal(err)
		}
		defer unmatchedProducer.Close()
	}
	// matched handle func
	wg := sync.WaitGroup{}
	asyncHandleDone := atomic.Uint64{}
	handleFunc := func(msg pulsar.ConsumerMessage, matched bool) bool {
		var producer pulsar.Producer
		// check to discard
		if matched {
			if matchedProducer == nil && cmdArgs.matchedAsDiscard {
				msg.Consumer.Nack(msg.Message)
				logrus.Infof("discard matched mid: %v\n", msg.ID())
			}
			producer = matchedProducer
		} else {
			if unmatchedProducer == nil && cmdArgs.unmatchedAsDiscard {
				logrus.Infof("discard unmatched mid: %v\n", msg.ID())
			}
			producer = unmatchedProducer
		}
		// prepare msg
		producerMsg := &pulsar.ProducerMessage{
			Payload:    msg.Payload(),
			Properties: msg.Properties(),
			EventTime:  msg.EventTime(),
		}
		// publish
		if !cmdArgs.publishBatchEnable { // sync
			producerMid, err := publish(producer, producerMsg, cmdArgs.publishMaxTimes)
			if err != nil {
				logrus.Fatalf("failed to send src mid: %v to destination", msg.ID())
			}
			err = msg.Consumer.Ack(msg.Message)
			if err != nil {
				logrus.Fatalf("failed to ack src mid: %v", msg.ID())
			}
			logrus.Infof("transferred src mid: %v to dest mid: %v", msg.ID(), producerMid)
		} else { // async
			wg.Add(1)
			publishAsync(producer, producerMsg, func(producerMid pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
				if err != nil {
					logrus.Fatalf("failed to send src mid: %v to destination", msg.ID())
				}
				logrus.Infof("async transferred src mid: %v to dest mid: %v", msg.ID(), producerMid)
				err = msg.Consumer.Ack(msg.Message)
				if err != nil {
					logrus.Fatalf("failed to ack src mid: %v", msg.ID())
				}
				asyncHandleDone.Add(1)
				wg.Done()
			}, cmdArgs.publishMaxTimes)
		}
		return true
	}

	logrus.Printf("started to tidy %v: (matched-to: %v, unmatched-to: %v)\n", mdlArgs.topic, cmdArgs.matchedTo, cmdArgs.unmatchedTo)
	logrus.Printf("conditions: %v\n", mdlArgs.condition)
	res := iterateInternalByConsumer(iterateOptions{
		brokerUrl:                    mdlArgs.BrokerUrl,
		topic:                        mdlArgs.topic,
		conditions:                   parsedMdlVars.conditions,
		startPublishTime:             parsedMdlVars.startPublishTime,
		startEventTime:               parsedMdlVars.startEventTime,
		printProgressIterateInterval: cmdArgs.printProgressIterateInterval,
	}, iterateConsumerOptions{
		subscription:   cmdArgs.subscription,
		iterateTimeout: cmdArgs.iterateTimeout,
		matchTimeout:   cmdArgs.matchTimeout,
	}, handleFunc)

	if cmdArgs.publishBatchEnable {
		logrus.Infof("recall done => %v, async done: %v\n", res.PrettyString(), asyncHandleDone.Load())
	} else {
		logrus.Infof("recall done => %v\n", res.PrettyString())
	}
}
