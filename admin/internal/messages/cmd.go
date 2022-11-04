package messages

import (
	"time"

	"github.com/antonmedv/expr/vm"
	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/support/constant"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type messagesArgs struct {
	BrokerUrl        string
	topic            string
	condition        string
	startPublishTime string // 通过消息时间比较, 非Reader Seek, 支持纳秒精度
	startEventTime   string // 通过消息时间比较, 非Reader Seek, 支持纳秒精度
	//startMid  string // 暂不支持, partitionedTopic需要逐一指定
}

func NewModuleCmd(rootArgs *internal.RootArgs) *cobra.Command {
	moduleArgs := &messagesArgs{}
	cmd := &cobra.Command{
		Use:   "messages ",
		Short: "Process messages such as iterate, recall and tidy.",
	}

	// parser variables
	flags := cmd.PersistentFlags()
	flags.StringVar(&moduleArgs.BrokerUrl, "broker-url", "pulsar://localhost:6650", "broker url")
	flags.StringVarP(&moduleArgs.condition, "condition", "c", "", constant.ConditionsUsage)
	flags.StringVar(&moduleArgs.startPublishTime, "start-publish-time", "", constant.StartPublishTimeUsage)
	flags.StringVar(&moduleArgs.startEventTime, "start-event-time", "", constant.StartEventTimeUsage)

	// add action commands
	cmd.AddCommand(newRecallCommand(rootArgs, moduleArgs))
	cmd.AddCommand(newIterateCommand(rootArgs, moduleArgs))
	cmd.AddCommand(newTidyCommand(rootArgs, moduleArgs))

	return cmd
}

type parsedMessagesArgs struct {
	conditions       []*vm.Program
	startPublishTime time.Time
	startEventTime   time.Time
}

func parseAndValidateMessagesVars(rtArgs *internal.RootArgs, mdlArgs *messagesArgs) *parsedMessagesArgs {
	// valid parameters
	if mdlArgs.topic == "" {
		logrus.Fatalf("missing source topic\n")
	}
	if mdlArgs.condition == "" {
		logrus.Fatalf("missing condition. please speicify one at least by -c or --conodition option\n")
	}
	startPublishTime := parseTimeString(mdlArgs.startPublishTime, "start-publish-time")
	startEventTime := parseTimeString(mdlArgs.startEventTime, "start-event-time")
	// compile conditions
	conditions := parseAndCompileConditions(mdlArgs.condition)
	// check src topics
	manager := admin.NewRobustTopicManager(rtArgs.Url)
	if _, err := manager.Stats(mdlArgs.topic); err != nil {
		logrus.Fatalf("invalid source topic: %v, err: %v\n", mdlArgs.topic, err)
	}
	return &parsedMessagesArgs{
		startPublishTime: startPublishTime,
		startEventTime:   startEventTime,
		conditions:       conditions,
	}
}
