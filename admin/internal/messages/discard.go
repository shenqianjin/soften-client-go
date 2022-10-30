package messages

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type discardArgs struct {
	printProgressIterateInterval uint64 // 每遍历多少个消息打印进度
	subscription                 string //
	iterateTimeout               uint32 // 终止条件: 能遍历到消息的超时时间
	matchTimeout                 uint32 // 终止条件: 有能匹配消息的超时时间
}

func newDiscardCommand(rtArgs *internal.RootArgs, mdlArgs *messagesArgs) *cobra.Command {
	cmdArgs := &discardArgs{}
	cmd := &cobra.Command{
		Use:   "discard ",
		Short: "discard soften topic or topics (Unrecommended)",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			mdlArgs.topic = args[0]
			discardMessages(rtArgs, mdlArgs, cmdArgs)
		},
	}
	// parse variables
	cmd.Flags().Uint64Var(&cmdArgs.printProgressIterateInterval, "print-progress-iterate-interval", 10000, util.PrintProgressIterateIntervalUsage)
	cmd.Flags().StringVarP(&cmdArgs.subscription, "subscription", "s", "", util.SingleSubscriptionUsage)
	cmd.Flags().Uint32Var(&cmdArgs.iterateTimeout, "iterate-timeout", 0, "iterate timeout")
	cmd.Flags().Uint32Var(&cmdArgs.matchTimeout, "match-timeout", 0, "match timeout")

	return cmd
}

func discardMessages(rtArgs *internal.RootArgs, mdlArgs *messagesArgs, cmdArgs *discardArgs) {
	// parse vars
	parsedMdlVars := parseAndValidateMessagesVars(rtArgs, mdlArgs)
	// valid subs
	if cmdArgs.subscription == "" {
		logrus.Fatalf("missing subscription option. please specify one with --subscription or -s\n")
	}
	// handle func
	stopChan := make(chan struct{})
	handleFunc := func(msg pulsar.ConsumerMessage) bool {
		err := msg.Consumer.Ack(msg.Message)
		for err != nil {
			logrus.Warnf("failed to ack message: %v will retry in 2s\n", formatMessage4Print(msg))
			time.Sleep(2 * time.Second)
			err = msg.Consumer.Ack(msg)
		}
		logrus.Infof("discarded msg: %v", msg.ID())
		return true
	}
	logrus.Infof("start to discard %v\n", mdlArgs.topic)
	logrus.Infof("conditions: %v\n", mdlArgs.condition)
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
	}, stopChan, handleFunc)
	logrus.Infof("iterate done => %v\n", res.PrettyString())
}
