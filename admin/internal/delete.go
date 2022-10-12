package internal

import (
	"fmt"

	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/spf13/cobra"
)

type deleteArgs struct {
	groundTopic  string
	status       string
	level        string
	subscription string
	partitioned  bool
	all          bool
}

func NewDeleteCommand(rtArgs rootArgs) *cobra.Command {
	cmdArgs := &deleteArgs{}
	cmd := &cobra.Command{
		Use:   "delete ",
		Short: "delete soften topic or topics",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs.groundTopic = args[0]
			deleteTopics(rtArgs, cmdArgs)
		},
	}
	// parse levels
	cmd.Flags().StringVarP(&cmdArgs.level, "level", "l", "", levelUsage)
	// parse statuses
	cmd.Flags().StringVarP(&cmdArgs.status, "status", "s", "", statusUsage)

	flags := cmd.Flags()
	// parse partition
	flags.BoolVarP(&cmdArgs.partitioned, "partitioned", "p", false, partitionedUsage)
	flags.StringVarP(&cmdArgs.subscription, "subscription", "S", "", subscriptionUsage)

	return cmd
}

func deleteTopics(rtArgs rootArgs, cmdArgs *deleteArgs) {
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

			partitioned: cmdArgs.partitioned,
			groundOnly:  false,
			readyOnly:   false,
		})
	}

	if err != nil {
		fmt.Printf("delete \"%s\" failed: %v\n", cmdArgs.groundTopic, err)
	}
	for _, topic := range topics {
		var err error
		if cmdArgs.partitioned {
			err = manager.PartitionedDelete(topic)
		} else {
			err = manager.Delete(topic)
		}
		if err != nil {
			fmt.Printf("deleted \"%s\" failed: %v\n", topic, err)
		} else {
			fmt.Printf("deleted \"%s\" successfully\n", topic)
		}
	}
}
