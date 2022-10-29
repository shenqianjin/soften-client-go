package messages

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/antonmedv/expr"
	"github.com/antonmedv/expr/vm"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/util"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type recallArgs struct {
	srcTopic  string
	destTopic string

	condition                    string
	printProgressIterateInterval uint64 // 每遍历多少个消息打印进度

	// src
	startPublishTime string // 通过消息时间比较, 非Reader Seek, 支持纳秒精度
	startEventTime   string // 通过消息时间比较, 非Reader Seek, 支持纳秒精度
	//startMid  string // 暂不支持, partitionedTopic需要逐一指定

	// desc
	batchEnable     bool
	publishMaxTimes uint64
}

func newRecallCommand(rtArgs *internal.RootArgs, mdlArgs *messagesArgs) *cobra.Command {
	cmdArgs := &recallArgs{}
	cmd := &cobra.Command{
		Use:   "recall ",
		Short: "delete soften topic or topics",
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs.srcTopic = args[0]
			cmdArgs.destTopic = args[1]
			recallMessages(*rtArgs, *mdlArgs, cmdArgs)
		},
	}
	// parse variables
	cmd.Flags().StringVarP(&cmdArgs.condition, "condition", "c", "", util.ConditionsUsage)
	cmd.Flags().Uint64Var(&cmdArgs.printProgressIterateInterval, "print-progress-iterate-interval", 10000, util.PrintProgressIterateIntervalUsage)
	// parse variables for src
	cmd.Flags().StringVar(&cmdArgs.startPublishTime, "start-publish-time", "", util.StartPublishTimeUsage)
	cmd.Flags().StringVar(&cmdArgs.startEventTime, "start-event-time", "", util.StartEventTimeUsage)
	// parse variables for dest
	cmd.Flags().BoolVarP(&cmdArgs.batchEnable, "batch", "b", false, util.BatchEnableUsage)
	cmd.Flags().Uint64Var(&cmdArgs.publishMaxTimes, "publish-max-times", 0, util.PublishMaxTimesUsage)

	return cmd
}

func recallMessages(rtArgs internal.RootArgs, mdlArgs messagesArgs, cmdArgs *recallArgs) {
	// valid parameters
	if cmdArgs.srcTopic == "" {
		logrus.Fatalf("missing source topic\n")
	}
	if cmdArgs.destTopic == "" {
		logrus.Fatalf("missing destination topic\n")
	}
	if cmdArgs.condition == "" {
		logrus.Fatalf("missing condition. please speicify one at least by -c or --conodition option\n")
	}
	startPublishTime := parseTimeString(cmdArgs.startPublishTime, "start-publish-time")
	startEventTime := parseTimeString(cmdArgs.startEventTime, "start-event-time")
	// compile conditions
	conditionPrograms := make([]*vm.Program, 0)
	conditions := strings.Split(cmdArgs.condition, ",")
	for _, condition := range conditions {
		if program, err := expr.Compile(condition); err != nil {
			logrus.Fatalf("invalid condition: %v, err: %v\n", condition, err)
		} else {
			conditionPrograms = append(conditionPrograms, program)
		}
	}
	// check src/dest topics
	manager := admin.NewTopicManager(rtArgs.Url)
	if _, err := manager.Stats(cmdArgs.srcTopic); err != nil {
		logrus.Fatalf("invalid source topic: %v, err: %v\n", cmdArgs.srcTopic, err)
	}
	if _, err := manager.Stats(cmdArgs.destTopic); err != nil {
		logrus.Fatalf("invalid destination topic: %v, err: %v\n", cmdArgs.destTopic, err)
	}
	// reader
	recallInternal(mdlArgs, cmdArgs, recallOptions{
		iterateOptions: iterateOptions{
			conditions:                   conditionPrograms,
			printProgressIterateInterval: cmdArgs.printProgressIterateInterval,
			startPublishTime:             startPublishTime,
			startEventTime:               startEventTime,
		},
		batchEnable:     cmdArgs.batchEnable,
		publishMaxTimes: cmdArgs.publishMaxTimes,
	})
}

type recallOptions struct {
	iterateOptions

	// dest
	batchEnable     bool
	publishMaxTimes uint64
}

func recallInternal(mdlArgs messagesArgs, cmdArgs *recallArgs, options recallOptions) {
	// src client
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: mdlArgs.BrokerUrl})
	if err != nil {
		logrus.Fatal(err)
	}
	defer client.Close()

	// dest client
	destClient, err := pulsar.NewClient(pulsar.ClientOptions{URL: mdlArgs.BrokerUrl})
	if err != nil {
		logrus.Fatal(err)
	}
	defer destClient.Close()

	// src reader
	reader, err := client.CreateReader(pulsar.ReaderOptions{
		Topic:          cmdArgs.srcTopic,
		StartMessageID: pulsar.EarliestMessageID(),
	})
	if err != nil {
		logrus.Fatal(err)
	}
	defer reader.Close()

	// desc producer
	destProducer, err := destClient.CreateProducer(pulsar.ProducerOptions{
		Topic: cmdArgs.destTopic,
	})
	if err != nil {
		logrus.Fatal(err)
	}
	defer destProducer.Close()

	// transfer by conditions
	logrus.Printf("started to recall %v to %v\n", cmdArgs.srcTopic, cmdArgs.destTopic)
	logrus.Printf("conditions: %v\n", cmdArgs.condition)
	recallTransferInternal(reader, destProducer, options)
	logrus.Printf("ended to recall %v to %v\n", cmdArgs.srcTopic, cmdArgs.destTopic)
}

func recallTransferInternal(reader pulsar.Reader, producer pulsar.Producer, options recallOptions) {
	recalled := atomic.Uint64{}
	iterated := atomic.Uint64{}
	var lastMsg pulsar.Message
	for reader.HasNext() {
		msg, err := reader.Next(context.Background())
		if err != nil {
			logrus.Fatal(err)
		}
		logrus.Debugf("started to transfer src mid: %v", msg.ID())
		iterated.Add(1)
		iteratedVal := iterated.Load()
		if options.printProgressIterateInterval > 0 && iteratedVal%options.printProgressIterateInterval == 0 {
			logrus.Infof("recall progress => iterated: %v, recalled: %v. next => mid: %v, publish time: %v, event time: %v\n",
				iteratedVal, recalled.Load(), msg.ID(), msg.PublishTime(), msg.EventTime())
		}
		// skip unmatched messages
		if !matched(msg, matchOptions{
			conditions:       options.conditions,
			startEventTime:   options.startEventTime,
			startPublishTime: options.startPublishTime}) {
			continue
		}
		// transfer
		producerMsg := &pulsar.ProducerMessage{
			Payload:    msg.Payload(),
			Properties: msg.Properties(),
			EventTime:  msg.EventTime(),
		}

		if !options.batchEnable { // sync
			producerMid, err := publish(producer, producerMsg, options.publishMaxTimes)
			if err != nil {
				logrus.Fatalf("failed to send src mid: %v to destination", msg.ID())
			}
			recalled.Add(1)
			logrus.Infof("transfer src mid: %v to dest mid: %v", msg.ID(), producerMid)
		} else { // async
			publishAsync(producer, producerMsg, func(producerMid pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
				if err != nil {
					logrus.Fatalf("failed to send src mid: %v to destination", msg.ID())
				}
				recalled.Add(1)
				logrus.Printf("async transfer src mid: %v to dest mid: %v", msg.ID(), producerMid)
			}, options.publishMaxTimes)
		}
	}
	logrus.Infof("recall done => iterated: %v, recalled: %v. last msg: %v\n", iterated.Load(), recalled.Load(), formatMessageAsString(lastMsg))
}

func publish(producer pulsar.Producer, producerMsg *pulsar.ProducerMessage, publishMaxTimes uint64) (pulsar.MessageID, error) {
	var mid pulsar.MessageID
	err := errors.New("dummy error")
	if publishMaxTimes <= 0 {
		// 无限重试
		for err != nil {
			mid, err = producer.Send(context.Background(), producerMsg)
		}
	} else {
		// 指定次数重试
		for i := uint64(1); err != nil && i <= publishMaxTimes; i++ {
			mid, err = producer.Send(context.Background(), producerMsg)
		}
	}
	return mid, err
}

func publishAsync(producer pulsar.Producer, producerMsg *pulsar.ProducerMessage,
	callback func(producerMid pulsar.MessageID, message *pulsar.ProducerMessage, err error),
	publishMaxTimes uint64) {
	callbackNew := func(producerMid pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
		if publishMaxTimes <= 0 {
			for err != nil {
				producerMid, err = producer.Send(context.Background(), producerMsg)
			}
		} else {
			for i := uint64(2); err != nil && i <= publishMaxTimes; i++ {
				producerMid, err = producer.Send(context.Background(), producerMsg)
			}
		}
		callback(producerMid, message, err)
	}
	producer.SendAsync(context.Background(), producerMsg, callbackNew)
}

// ------ helper ------

type matchOptions struct {
	conditions       []*vm.Program
	startPublishTime time.Time
	startEventTime   time.Time
}

func matched(msg pulsar.Message, options matchOptions) bool {
	// old publish time
	if !options.startPublishTime.IsZero() {
		if msg.PublishTime().Before(options.startPublishTime) {
			return false
		}
	}
	// old event time
	if !options.startEventTime.IsZero() && !msg.EventTime().IsZero() {
		if msg.EventTime().Before(options.startEventTime) {
			return false
		}
	}
	// check conditions
	for conditionIndex, program := range options.conditions {
		output, err1 := expr.Run(program, msg)
		if err1 != nil {
			logrus.Fatal("failed to check condition: %v, err: %v", conditionIndex, err1)
		}
		if output == true {
			return true
		}
	}
	return false
}
