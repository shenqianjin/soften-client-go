package messages

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/support/constant"
	util2 "github.com/shenqianjin/soften-client-go/admin/internal/support/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type iterateArgs struct {
	printProgressIterateInterval uint64 // 每遍历多少个消息打印进度
	printMode                    uint   // 命中输出模式
}

func newIterateCommand(rtArgs *internal.RootArgs, mdlArgs *messagesArgs) *cobra.Command {
	cmdArgs := &iterateArgs{}
	cmd := &cobra.Command{
		Use:   "iterate ",
		Short: "Iterate messages of a source topic and print matched ones with conditions.",
		Long: "Iterate messages of a source topic and print matched ones with conditions.\n" +
			"\n" +
			"Exact 1 argument like the below format is necessary: \n" +
			"  <schema>://<tenant>/<namespace>/<topic>\n" +
			"  <tenant>/<namespace>/<topic>\n" +
			"  <topic>",
		Example: "(1) soften-admin messages iterate test -c '" + SampleConditionAgeLessEqualThan10 + "'\n" +
			"(2) soften-admin messages iterate public/default/test -c '" + SampleConditionUidRangeAndNameStartsWithNo12 + "'\n" +
			"(3) soften-admin messages iterate persistent://business/finance/equity -c '" + SampleConditionSpouseAgeLessThan40 + "'\n" +
			"(4) soften-admin messages iterate test -c '" + SampleConditionFriendsHasOneOfAgeLessEqualThan10 + "'\n" +
			"(5) soften-admin messages iterate test -c '" + SampleConditionAgeLessEqualThan10OrNameStartsWithNo12 + "'",
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			mdlArgs.topic = args[0]
			iterateMessages(rtArgs, mdlArgs, cmdArgs)
		},
	}
	// parse variables
	cmd.Flags().Uint64Var(&cmdArgs.printProgressIterateInterval, "print-progress-iterate-interval", 10000, constant.PrintProgressIterateIntervalUsage)
	cmd.Flags().UintVar(&cmdArgs.printMode, "print-mode", 0, constant.PrintModeUsage)
	return cmd
}

func iterateMessages(rtArgs *internal.RootArgs, mdlArgs *messagesArgs, cmdArgs *iterateArgs) {
	// parse vars
	parsedMdlVars := parseAndValidateMessagesVars(rtArgs, mdlArgs)
	// iterator handle func
	var lastMsg pulsar.Message
	handleFunc := func(msg pulsar.Message) bool {
		// mark last message
		if lastMsg == nil {
			lastMsg = msg
		}
		switch cmdArgs.printMode {
		case 1:
			logrus.Printf("matched msg: %v", msg.ID())
		case 2:
			logrus.Printf("matched msg: %v", util2.FormatMessage4Print(msg))
		default:
			// print nothing
		}
		return true
	}
	// iterate
	logrus.Infof("start to iterate %v\n", mdlArgs.topic)
	logrus.Infof("conditions: %v\n", mdlArgs.condition)
	res := iterateInternalByReader(iterateOptions{
		brokerUrl:                    mdlArgs.BrokerUrl,
		webUrl:                       mdlArgs.WebUrl,
		topic:                        mdlArgs.topic,
		conditions:                   parsedMdlVars.conditions,
		startPublishTime:             parsedMdlVars.startPublishTime,
		startEventTime:               parsedMdlVars.startEventTime,
		printProgressIterateInterval: cmdArgs.printProgressIterateInterval,
	}, handleFunc)
	logrus.Infof("iterate done => \n%v\n", res.PrettyString())

}
