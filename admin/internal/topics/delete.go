package topics

import (
	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/util"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type deleteArgs struct {
	groundTopic  string
	status       string
	level        string
	subscription string
	partitioned  bool
}

func newDeleteCommand(rtArgs *internal.RootArgs) *cobra.Command {
	cmdArgs := &deleteArgs{}
	cmd := &cobra.Command{
		Use:   "delete ",
		Short: "delete soften topic or topics",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs.groundTopic = args[0]
			deleteTopics(*rtArgs, cmdArgs)
		},
	}
	// parse levels
	cmd.Flags().StringVarP(&cmdArgs.level, "level", "l", "", util.LevelUsage)
	// parse statuses
	cmd.Flags().StringVarP(&cmdArgs.status, "status", "s", "", util.StatusUsage)

	flags := cmd.Flags()
	// parse partition
	flags.BoolVarP(&cmdArgs.partitioned, "partitioned", "p", false, util.PartitionedUsage)
	flags.StringVarP(&cmdArgs.subscription, "subscription", "S", "", util.SubscriptionUsage)

	return cmd
}

func deleteTopics(rtArgs internal.RootArgs, cmdArgs *deleteArgs) {
	manager := admin.NewTopicManager(rtArgs.Url)

	var topics []string
	var err error
	if cmdArgs.level != "" || cmdArgs.status != "" {
		topics = util.FormatTopics(cmdArgs.groundTopic, cmdArgs.level, cmdArgs.status, cmdArgs.subscription)
	} else {
		topics, err = listAndCheckTopicsByOptions(listOptions{
			url:          rtArgs.Url,
			groundTopic:  cmdArgs.groundTopic,
			subscription: cmdArgs.subscription,

			partitioned: cmdArgs.partitioned,
			groundOnly:  false,
			readyOnly:   false,
		})
	}
	if err != nil {
		logrus.Fatalf("delete \"%s\" failed: %v\n", cmdArgs.groundTopic, err)
	}
	// delete one by one
	for _, topic := range topics {
		var err error
		if cmdArgs.partitioned {
			err = manager.PartitionedDelete(topic)
		} else {
			err = manager.Delete(topic)
		}
		if err != nil {
			logrus.Fatalf("deleted \"%s\" failed: %v\n", topic, err)
		} else {
			logrus.Infof("deleted \"%s\" successfully\n", topic)
		}
	}
}
