package messages

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/support/constant"
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
	endPublishTime               string // 终止条件: 发布时间, 支持纳秒精度
	endEventTime                 string // 终止条件: 事件时间, 支持纳秒精度

	publishBatchEnable bool
	publishMaxTimes    uint64
}

func newTidyCommand(rtArgs *internal.RootArgs, mdlArgs *messagesArgs) *cobra.Command {
	cmdArgs := &tidyArgs{}
	cmd := &cobra.Command{
		Use:   "tidy ",
		Short: "Tidy messages of an topic by discarding or transferring.",
		Long: "Tidy messages of an topic by discarding or transferring.\n" +
			"Matched ones with condition can be transferred to another topic or discarded.\n" +
			"So does unmatched ones.\n" +
			"\n" +
			"Exact 1 argument like the below format is necessary: \n" +
			"  <schema>://<tenant>/<namespace>/<topic>\n" +
			"  <tenant>/<namespace>/<topic>\n" +
			"  <topic>",

		Example: "(1) soften-admin messages tidy test01 -S sub -c '" + SampleConditionAgeLessEqualThan10 + "' --matched-to=test02 --unmatched-to=test01\n" +
			"(2) soften-admin messages tidy public/default/test01 -S sub -c '" + SampleConditionUidRangeAndNameStartsWithNo12 + "' --matched-as-discard --unmatched-to=test01\n" +
			"(3) soften-admin messages tidy persistent://business/finance/equity -S sub -c '" + SampleConditionSpouseAgeLessThan40 + "' --matched-to=test02 --unmatched-as-discard\n" +
			"(4) soften-admin messages tidy test03 -S sub -c '" + SampleConditionFriendsHasOneOfAgeLessEqualThan10 + "' --matched-to=test02 --unmatched-to=test03\n" +
			"(5) soften-admin messages tidy test03 -S sub -c '" + SampleConditionAgeLessEqualThan10OrNameStartsWithNo12 + "' --matched-to=test02 --unmatched-to=test03",
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			mdlArgs.topic = args[0]
			tidyMessages(rtArgs, mdlArgs, cmdArgs)
		},
	}
	// parse variables for source
	cmd.Flags().Uint64Var(&cmdArgs.printProgressIterateInterval, "print-progress-iterate-interval", 10000, constant.PrintProgressIterateIntervalUsage)
	cmd.Flags().StringVarP(&cmdArgs.subscription, "subscription", "S", "", constant.SingleSubscriptionUsage)
	cmd.Flags().Uint32Var(&cmdArgs.iterateTimeout, "iterate-timeout", 12, "iterate timeout")
	cmd.Flags().Uint32Var(&cmdArgs.matchTimeout, "match-timeout", 0, "match timeout")
	cmd.Flags().StringVar(&cmdArgs.endPublishTime, "end-publish-time", "now()", constant.EndPublishTimeUsage)
	cmd.Flags().StringVar(&cmdArgs.endEventTime, "end-event-time", "", constant.EndEventTimeUsage)

	// parse variables for destination
	cmd.Flags().StringVar(&cmdArgs.matchedTo, "matched-to", "", "topic to publish these matched messages to")
	cmd.Flags().StringVar(&cmdArgs.unmatchedTo, "unmatched-to", "", "topic to publish these unmatched messages to")
	cmd.Flags().BoolVar(&cmdArgs.matchedAsDiscard, "matched-as-discard", false, "whether discards these matched messages, it is ignored if '--matched-to' is not empty")
	cmd.Flags().BoolVar(&cmdArgs.unmatchedAsDiscard, "unmatched-as-discard", false, "whether discards these unmatched messages, it is ignored if '--unmatched-to' is not empty")
	cmd.Flags().BoolVarP(&cmdArgs.publishBatchEnable, "publish-batch-enable", "b", false, constant.BatchEnableUsage)
	cmd.Flags().Uint64Var(&cmdArgs.publishMaxTimes, "publish-max-times", 1, constant.PublishMaxTimesUsage)

	return cmd
}

func tidyMessages(rtArgs *internal.RootArgs, mdlArgs *messagesArgs, cmdArgs *tidyArgs) {
	// parse vars
	parsedMdlVars := parseAndValidateMessagesVars(rtArgs, mdlArgs)
	// valid subs
	if cmdArgs.subscription == "" {
		logrus.Fatalf("missing subscription option. please specify one with --subscription or -S\n")
	} else if strings.Contains(cmdArgs.subscription, ",") {
		logrus.Fatalf("multiple subscription names are not supported for this command\n")
	}
	endPublishTime := parseTimeString(cmdArgs.endPublishTime, "end-publish-time")
	endEventTime := parseTimeString(cmdArgs.endEventTime, "end-event-time")
	// check dest topics
	if cmdArgs.matchedTo != "" {
		manager := admin.NewRobustTopicManager(rtArgs.WebUrl)
		if _, err := manager.Stats(cmdArgs.matchedTo); err != nil {
			logrus.Fatalf("invalid 'matched-to' destination topic: %v, err: %v\n", cmdArgs.matchedTo, err)
		}
	} else if !cmdArgs.matchedAsDiscard {
		logrus.Fatalf("missing 'matched-to' destination. please specify one by '--matched-to' or enable '--matched-as-discard'")
	}
	if cmdArgs.unmatchedTo != "" {
		manager := admin.NewRobustTopicManager(rtArgs.WebUrl)
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
				err := msg.Consumer.Ack(msg.Message)
				if err != nil {
					logrus.Fatal(err)
				}
				logrus.Infof("discard matched mid: %v\n", msg.ID())
				return true
			}
			producer = matchedProducer
		} else {
			if unmatchedProducer == nil && cmdArgs.unmatchedAsDiscard {
				msg.Consumer.Ack(msg.Message)
				if err != nil {
					logrus.Fatal(err)
				}
				logrus.Infof("discard unmatched mid: %v\n", msg.ID())
				return true
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
	res := iterateByConsumer(iterateOptions{
		brokerUrl:                    mdlArgs.BrokerUrl,
		webUrl:                       mdlArgs.WebUrl,
		topic:                        mdlArgs.topic,
		conditions:                   parsedMdlVars.conditions,
		startPublishTime:             parsedMdlVars.startPublishTime,
		startEventTime:               parsedMdlVars.startEventTime,
		printProgressIterateInterval: cmdArgs.printProgressIterateInterval,
	}, iterateConsumerOptions{
		subscription:   cmdArgs.subscription,
		iterateTimeout: cmdArgs.iterateTimeout,
		matchTimeout:   cmdArgs.matchTimeout,
		endPublishTime: endPublishTime,
		endEventTime:   endEventTime,
	}, handleFunc)
	// wait send async callback
	wg.Wait()

	if cmdArgs.publishBatchEnable {
		logrus.Infof("recall done =>>>>>> \n%v, async done: %v\n", res.PrettyString(), asyncHandleDone.Load())
	} else {
		logrus.Infof("recall done =>>>>>> \n%v\n", res.PrettyString())
	}
}
