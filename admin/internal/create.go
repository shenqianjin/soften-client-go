package internal

import (
	"fmt"

	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/spf13/cobra"
)

type createArgs struct {
	groundTopic  string
	status       string
	level        string
	partitions   uint
	subscription string
}

func NewCreateCommand(rtArgs rootArgs) *cobra.Command {
	cmdArgs := createArgs{}
	cmd := &cobra.Command{
		Use:   "create ",
		Short: "create soften topic or topics",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs.groundTopic = args[0]
			createTopics(rtArgs, &cmdArgs)
		},
	}
	// parse levels
	cmd.Flags().StringVarP(&cmdArgs.level, "level", "l", "L1", levelUsage)
	// parse statuses
	cmd.Flags().StringVarP(&cmdArgs.status, "status", "s", "Ready", statusUsage)

	flags := cmd.Flags()
	// parse partition
	flags.UintVarP(&cmdArgs.partitions, "partitions", "p", 0, partitionsUsage4Create)
	flags.StringVarP(&cmdArgs.subscription, "subscription", "S", "", subscriptionUsage)

	return cmd
}

func createTopics(rtArgs rootArgs, cmdArgs *createArgs) {
	manager := admin.NewTopicManager(rtArgs.url)

	topics := formatTopics(cmdArgs.groundTopic, cmdArgs.level, cmdArgs.status, cmdArgs.subscription)
	for _, topic := range topics {
		var err error
		if cmdArgs.partitions <= 0 {
			err = manager.Create(topic)
		} else {
			err = manager.PartitionedCreate(topic, cmdArgs.partitions)
		}
		if err != nil {
			fmt.Printf("created \"%s\" failed: %v\n", topic, err)
		} else {
			fmt.Printf("created \"%s\" successfully\n", topic)
		}
	}
}
