package topics

import (
	"fmt"
	"os"

	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/util"

	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/spf13/cobra"
)

type updateArgs struct {
	groundTopic  string
	status       string
	level        string
	subscription string
	partitions   uint
}

func newUpdateCommand(rtArgs internal.RootArgs) *cobra.Command {
	cmdArgs := &updateArgs{}
	cmd := &cobra.Command{
		Use:   "update ",
		Short: "update soften topic or topics",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs.groundTopic = args[0]
			updateTopics(rtArgs, cmdArgs)
		},
	}
	// parse levels
	cmd.Flags().StringVarP(&cmdArgs.level, "level", "l", "", util.LevelUsage)
	// parse statuses
	cmd.Flags().StringVarP(&cmdArgs.status, "status", "s", "", util.StatusUsage)
	cmd.Flags().StringVarP(&cmdArgs.subscription, "subscription", "S", "", util.SubscriptionUsage)
	cmd.Flags().UintVarP(&cmdArgs.partitions, "partitioned", "p", 0, util.PartitionsUsage4Update)

	return cmd
}

func updateTopics(rtArgs internal.RootArgs, cmdArgs *updateArgs) {
	if cmdArgs.partitions <= 0 {
		fmt.Println("please specify the partitions (with -p or --partitions options) " +
			"and make sure it is more than the original value")
		os.Exit(1)
	}
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

			partitioned: true,
			groundOnly:  false,
			readyOnly:   false,
		})
	}

	if err != nil {
		fmt.Printf("updated \"%s\" failed: %v\n", cmdArgs.groundTopic, err)
	}
	for _, topic := range topics {
		err := manager.PartitionedUpdate(topic, cmdArgs.partitions)
		if err != nil {
			fmt.Printf("updated \"%s\" failed: %v\n", topic, err)
		} else {
			fmt.Printf("updated \"%s\" successfully, partitions is %v now\n", topic, cmdArgs.partitions)
		}
	}
}
