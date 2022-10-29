package messages

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/antonmedv/expr/vm"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/util"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type iterateArgs struct {
	topic string

	condition                    string
	printProgressIterateInterval uint64 // 每遍历多少个消息打印进度
	printMode                    uint   // 命中输出模式

	// src
	startPublishTime string // 通过消息时间比较, 非Reader Seek, 支持纳秒精度
	startEventTime   string // 通过消息时间比较, 非Reader Seek, 支持纳秒精度
}

func newIterateCommand(rtArgs *internal.RootArgs, mdlArgs *messagesArgs) *cobra.Command {
	cmdArgs := &iterateArgs{}
	cmd := &cobra.Command{
		Use:   "iterate ",
		Short: "iterate soften topic or topics",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs.topic = args[0]
			iterateMessages(*rtArgs, *mdlArgs, cmdArgs)
		},
	}
	// parse variables
	cmd.Flags().StringVarP(&cmdArgs.condition, "condition", "c", "", util.ConditionsUsage)
	cmd.Flags().Uint64Var(&cmdArgs.printProgressIterateInterval, "print-progress-iterate-interval", 10000, util.PrintProgressIterateIntervalUsage)
	cmd.Flags().UintVar(&cmdArgs.printMode, "print-mode", 0, util.PrintModeUsage)
	// parse variables for src
	cmd.Flags().StringVar(&cmdArgs.startPublishTime, "start-publish-time", "", util.StartPublishTimeUsage)
	cmd.Flags().StringVar(&cmdArgs.startEventTime, "start-event-time", "", util.StartEventTimeUsage)

	return cmd
}

func iterateMessages(rtArgs internal.RootArgs, mdlArgs messagesArgs, cmdArgs *iterateArgs) {
	// valid parameters
	if cmdArgs.topic == "" {
		logrus.Fatalf("missing source topic\n")
	}
	if cmdArgs.condition == "" {
		logrus.Fatalf("missing condition. please speicify one at least by -c or --conodition option\n")
	}
	startPublishTime := parseTimeString(cmdArgs.startPublishTime, "start-publish-time")
	startEventTime := parseTimeString(cmdArgs.startEventTime, "start-event-time")
	// compile conditions
	conditions := parseAndCompileConditions(cmdArgs.condition)
	// check src/dest topics
	manager := admin.NewTopicManager(rtArgs.Url)
	if _, err := manager.Stats(cmdArgs.topic); err != nil {
		logrus.Fatalf("invalid source topic: %v, err: %v\n", cmdArgs.topic, err)
	}
	// reader
	iterateInternal(mdlArgs, cmdArgs, iterateOptions{
		conditions:                   conditions,
		printProgressIterateInterval: cmdArgs.printProgressIterateInterval,
		printMode:                    cmdArgs.printMode,
		startPublishTime:             startPublishTime,
		startEventTime:               startEventTime,
	})
}

type iterateOptions struct {
	conditions                   []*vm.Program
	printProgressIterateInterval uint64
	printMode                    uint

	// src
	startPublishTime time.Time
	startEventTime   time.Time
}

func iterateInternal(mdlArgs messagesArgs, cmdArgs *iterateArgs, options iterateOptions) {
	// src client
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: mdlArgs.BrokerUrl})
	if err != nil {
		logrus.Fatal(err)
	}
	defer client.Close()

	// src reader
	reader, err := client.CreateReader(pulsar.ReaderOptions{
		Topic:          cmdArgs.topic,
		StartMessageID: pulsar.EarliestMessageID(),
	})
	if err != nil {
		logrus.Fatal(err)
	}
	defer reader.Close()

	// transfer by conditions
	hit := atomic.Uint64{}
	iterated := atomic.Uint64{}
	var lastMsg pulsar.Message
	logrus.Infof("start to detect %v to %v\n", cmdArgs.topic)
	logrus.Infof("conditions: %v\n", cmdArgs.condition)
	for reader.HasNext() {
		msg, err := reader.Next(context.Background())
		if err != nil {
			logrus.Fatal(err)
		}
		logrus.Debugf("started to transfer src mid: %v", msg.ID())
		iterated.Add(1)
		iteratedVal := iterated.Load()
		if options.printProgressIterateInterval > 0 && iteratedVal%options.printProgressIterateInterval == 0 {
			logrus.Infof("iterate progress => iterated: %v, hit: %v. next => mid: %v, publish time: %v, event time: %v\n",
				iteratedVal, hit.Load(), msg.ID(), msg.PublishTime(), msg.EventTime())
		}
		// skip unmatched messages
		if !matched(msg, matchOptions{
			conditions:       options.conditions,
			startEventTime:   options.startEventTime,
			startPublishTime: options.startPublishTime}) {
			continue
		}
		hit.Add(1)
		switch options.printMode {
		case 1:
			logrus.Printf("matched msg: %v", formatMessageAsString(msg))
		case 2:
			logrus.Printf("matched msg: %v", msg.ID())
		default:
			// print nothing
		}
	}
	logrus.Infof("iterate done => iterated: %v, hit: %v. last msg: %v\n", iterated.Load(), hit.Load(), formatMessageAsString(lastMsg))

}
