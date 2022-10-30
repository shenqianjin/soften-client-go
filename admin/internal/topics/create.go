package topics

import (
	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/util"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type createArgs struct {
	groundTopic  string
	status       string
	level        string
	partitions   uint
	subscription string
}

func newCreateCommand(rtArgs *internal.RootArgs) *cobra.Command {
	cmdArgs := createArgs{}
	cmd := &cobra.Command{
		Use:   "create ",
		Short: "create soften topic or topics",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs.groundTopic = args[0]
			createTopics(*rtArgs, &cmdArgs)
		},
	}
	// parse levels
	cmd.Flags().StringVarP(&cmdArgs.level, "level", "l", "L1", util.LevelUsage)
	// parse statuses
	cmd.Flags().StringVarP(&cmdArgs.status, "status", "s", "Ready", util.StatusUsage)

	flags := cmd.Flags()
	// parse partition
	flags.UintVarP(&cmdArgs.partitions, "partitions", "p", 0, util.PartitionsUsage4Create)
	flags.StringVarP(&cmdArgs.subscription, "subscription", "S", "", util.SubscriptionUsage)

	return cmd
}

func createTopics(rtArgs internal.RootArgs, cmdArgs *createArgs) {
	manager := admin.NewRobustTopicManager(rtArgs.Url)

	topics := util.FormatTopics(cmdArgs.groundTopic, cmdArgs.level, cmdArgs.status, cmdArgs.subscription)
	for _, topic := range topics {
		var err error
		err = manager.Create(topic, cmdArgs.partitions)
		if err != nil {
			logrus.Fatalf("created \"%s\" failed: %v\n", topic, err)
		} else {
			logrus.Infof("created \"%s\" successfully\n", topic)
		}
	}
}
