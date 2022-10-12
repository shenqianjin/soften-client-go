package internal

import (
	"fmt"
	"os"

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

func NewUpdateCommand(rtArgs rootArgs) *cobra.Command {
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
	cmd.Flags().StringVarP(&cmdArgs.level, "level", "l", "", levelUsage)
	// parse statuses
	cmd.Flags().StringVarP(&cmdArgs.status, "status", "s", "", statusUsage)
	cmd.Flags().StringVarP(&cmdArgs.subscription, "subscription", "S", "", subscriptionUsage)
	cmd.Flags().UintVarP(&cmdArgs.partitions, "partitioned", "p", 0, partitionsUsage4Update)

	return cmd
}

func updateTopics(rtArgs rootArgs, cmdArgs *updateArgs) {
	if cmdArgs.partitions <= 0 {
		fmt.Println("please specify the partitions (with -p or --partitions options) " +
			"and make sure it is more than the original value")
		os.Exit(1)
	}
	manager := admin.NewTopicManager(rtArgs.url)

	var topics []string
	var err error
	if cmdArgs.level != "" || cmdArgs.status != "" {
		topics = formatTopics(cmdArgs.groundTopic, cmdArgs.level, cmdArgs.status, cmdArgs.subscription)
	} else {
		topics, err = listAndCheckTopicsByOptions(listOptions{
			url:          rtArgs.url,
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
