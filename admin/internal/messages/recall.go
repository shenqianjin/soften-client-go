package messages

import (
	"sync"
	"sync/atomic"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/util"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type recallArgs struct {
	destTopic string

	printProgressIterateInterval uint64 // 每遍历多少个消息打印进度
	publishBatchEnable           bool
	publishMaxTimes              uint64
}

func newRecallCommand(rtArgs *internal.RootArgs, mdlArgs *messagesArgs) *cobra.Command {
	cmdArgs := &recallArgs{}
	cmd := &cobra.Command{
		Use:   "recall ",
		Short: "recall messages from a source topic (DQL generally and not only) to anther topic",
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			mdlArgs.topic = args[0]
			cmdArgs.destTopic = args[1]
			recallMessages(rtArgs, mdlArgs, cmdArgs)
		},
	}
	// parse variables
	cmd.Flags().Uint64Var(&cmdArgs.printProgressIterateInterval, "print-progress-iterate-interval", 10000, util.PrintProgressIterateIntervalUsage)
	cmd.Flags().BoolVarP(&cmdArgs.publishBatchEnable, "publish-batch-enable", "b", false, util.BatchEnableUsage)
	cmd.Flags().Uint64Var(&cmdArgs.publishMaxTimes, "publish-max-times", 0, util.PublishMaxTimesUsage)

	return cmd
}

func recallMessages(rtArgs *internal.RootArgs, mdlArgs *messagesArgs, cmdArgs *recallArgs) {
	// parse vars
	parsedMdlVars := parseAndValidateMessagesVars(rtArgs, mdlArgs)
	// check src/dest topics
	manager := admin.NewRobustTopicManager(rtArgs.Url)
	if _, err := manager.Stats(cmdArgs.destTopic); err != nil {
		logrus.Fatalf("invalid destination topic: %v, err: %v\n", cmdArgs.destTopic, err)
	}
	// dest client
	destClient, err := pulsar.NewClient(pulsar.ClientOptions{URL: mdlArgs.BrokerUrl})
	if err != nil {
		logrus.Fatal(err)
	}
	defer destClient.Close()
	// desc producer
	destProducer, err := destClient.CreateProducer(pulsar.ProducerOptions{
		Topic: cmdArgs.destTopic,
	})
	if err != nil {
		logrus.Fatal(err)
	}
	defer destProducer.Close()
	// handle func
	wg := sync.WaitGroup{}
	asyncHandleDone := atomic.Uint64{}
	handleFunc := func(msg pulsar.Message) bool {
		// prepare msg
		producerMsg := &pulsar.ProducerMessage{
			Payload:    msg.Payload(),
			Properties: msg.Properties(),
			EventTime:  msg.EventTime(),
		}
		// publish
		if !cmdArgs.publishBatchEnable { // sync
			producerMid, err := publish(destProducer, producerMsg, cmdArgs.publishMaxTimes)
			if err != nil {
				logrus.Fatalf("failed to send src mid: %v to destination", msg.ID())
			}
			logrus.Infof("transferred src mid: %v to dest mid: %v", msg.ID(), producerMid)
		} else { // async
			wg.Add(1)
			publishAsync(destProducer, producerMsg, func(producerMid pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
				if err != nil {
					logrus.Fatalf("failed to send src mid: %v to destination", msg.ID())
				}
				logrus.Infof("async transferred src mid: %v to dest mid: %v", msg.ID(), producerMid)
				asyncHandleDone.Add(1)
				wg.Done()
			}, cmdArgs.publishMaxTimes)
		}
		return true
	}
	// iterate to recall
	logrus.Printf("started to recall %v to %v\n", mdlArgs.topic, cmdArgs.destTopic)
	logrus.Printf("conditions: %v\n", mdlArgs.condition)
	res := iterateInternalByReader(iterateOptions{
		brokerUrl:                    mdlArgs.BrokerUrl,
		topic:                        mdlArgs.topic,
		conditions:                   parsedMdlVars.conditions,
		startPublishTime:             parsedMdlVars.startPublishTime,
		startEventTime:               parsedMdlVars.startEventTime,
		printProgressIterateInterval: cmdArgs.printProgressIterateInterval,
	}, handleFunc)
	wg.Wait()
	if cmdArgs.publishBatchEnable {
		logrus.Infof("recall done => %v, async done: %v\n", res.PrettyString(), asyncHandleDone.Load())
	} else {
		logrus.Infof("recall done => %v\n", res.PrettyString())
	}
}
