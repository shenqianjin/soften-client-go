package messages

import (
	"errors"
	"fmt"

	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/util"
	"github.com/spf13/cobra"
)

type recallArgs struct {
	srcGroundTopic  string
	srcStatus       string
	srcLevel        string
	srcSubscription string
	srcPartitioned  bool

	destGroundTopic  string
	destStatus       string
	destLevel        string
	destSubscription string
	destPartitioned  bool

	condition string
}

func newRecallCommand(rtArgs internal.RootArgs) *cobra.Command {
	cmdArgs := &recallArgs{}
	cmd := &cobra.Command{
		Use:   "recall ",
		Short: "delete soften topic or topics",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs.srcGroundTopic = args[0]
			recallMessages(rtArgs, cmdArgs)
		},
	}
	// parse variables
	cmd.Flags().StringVarP(&cmdArgs.condition, "conditions", "c", "", util.ConditionsUsage)

	cmd.Flags().StringVar(&cmdArgs.srcLevel, "srcLevel", "", "source "+util.LevelUsage)
	cmd.Flags().StringVar(&cmdArgs.srcStatus, "srcStatus", "", "source "+util.StatusUsage)
	cmd.Flags().BoolVar(&cmdArgs.srcPartitioned, "srcPartitioned", false, "source "+util.PartitionedUsage)
	cmd.Flags().StringVar(&cmdArgs.srcSubscription, "srcSubscription", "", "source "+util.SubscriptionUsage)

	cmd.Flags().StringVar(&cmdArgs.destLevel, "destLevel", "", "destination "+util.SingleLevelUsage)
	cmd.Flags().StringVar(&cmdArgs.destStatus, "destStatus", "", "destination "+util.StatusUsage)
	cmd.Flags().BoolVar(&cmdArgs.destPartitioned, "destPartitioned", false, "destination "+util.PartitionedUsage)
	cmd.Flags().StringVar(&cmdArgs.destSubscription, "destSubscription", "", "destination "+util.SubscriptionUsage)

	return cmd
}

func recallMessages(rtArgs internal.RootArgs, cmdArgs *recallArgs) {
	//manager := admin.NewTopicManager(rtArgs.Url)

	srcTopics := util.FormatTopics(cmdArgs.srcGroundTopic, cmdArgs.srcLevel, cmdArgs.srcSubscription, cmdArgs.srcSubscription)
	destTopics := util.FormatTopics(cmdArgs.destGroundTopic, cmdArgs.destLevel, cmdArgs.destSubscription, cmdArgs.destSubscription)

	if len(destTopics) != 1 {
		err := errors.New("more than onre destination topics parsed from your dest options")
		fmt.Printf("recall \"%s\" to \"%s\" failed: %v\n", cmdArgs.srcGroundTopic, cmdArgs.destGroundTopic, err)
	}
	for _, topic := range srcTopics {
		// recall
		var err error
		if err != nil {
			fmt.Printf("recall \"%s\" to \"%s\" failed: %v\n", topic, err)
		} else {
			fmt.Printf("recall \"%s\" to \"%s\" successfully\n", topic)
		}
	}
}
