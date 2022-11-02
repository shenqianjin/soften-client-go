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
		Short: "Create soften topic or topics based-on ground topic.",
		Long: "Create soften topic or topics based-on ground topic.\n" +
			"\n" +
			"Exact 1 argument like the below format is necessary: \n" +
			"  <schema>://<tenant>/<namespace>/<topic>\n" +
			"  <tenant>/<namespace>/<topic>\n" +
			"  <topic>",
		Example: "(1) soften-admin topics create test\n" +
			"(2) soften-admin topics create public/default/test -P\n" +
			"(3) soften-admin topics create persistent://business/finance/equity -Pp 8",
		Args: cobra.ExactArgs(1),
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
	// format and validate topics
	topics := util.FormatTopics(cmdArgs.groundTopic, mdlArgs.level, mdlArgs.status, mdlArgs.subscription)
	// validate partitions
	if cmdArgs.partitioned {
		if cmdArgs.partitions < 1 {
			logrus.Fatalf("the number of partitioned topics must be equal or larger than 1")
		}
	} else {
		if cmdArgs.partitions != 1 {
			logrus.Fatalf("the number of non-partitioned topics must be 1")
		}
	}
	// create
	manager := admin.NewRobustTopicManager(rtArgs.Url)
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
