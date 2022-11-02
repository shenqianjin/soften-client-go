package topics

import (
	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/util"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type createArgs struct {
	groundTopic string
	partitioned bool
	partitions  uint
}

func newCreateCommand(rtArgs *internal.RootArgs, mdlArgs *topicsArgs) *cobra.Command {
	cmdArgs := &createArgs{}
	cmd := &cobra.Command{
		Use:   "create ",
		Short: "Create soften topic or topics.",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs.groundTopic = args[0]
			createTopics(rtArgs, mdlArgs, cmdArgs)
		},
	}

	// parse partition
	cmd.Flags().BoolVarP(&cmdArgs.partitioned, "partitioned", "P", false, util.PartitionedUsage)
	cmd.Flags().UintVarP(&cmdArgs.partitions, "partitions", "p", 1, util.PartitionsUsage4Create)

	return cmd
}

func createTopics(rtArgs *internal.RootArgs, mdlArgs *topicsArgs, cmdArgs *createArgs) {
	manager := admin.NewRobustTopicManager(rtArgs.Url)

	topics := util.FormatTopics(cmdArgs.groundTopic, mdlArgs.level, mdlArgs.status, mdlArgs.subscription)
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
