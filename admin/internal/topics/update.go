package topics

import (
	"errors"

	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/util"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"
)

type updateArgs struct {
	groundTopic string
	partitions  uint
	all         bool
}

func newUpdateCommand(rtArgs *internal.RootArgs, mdlArgs *topicsArgs) *cobra.Command {
	cmdArgs := &updateArgs{}
	cmd := &cobra.Command{
		Use:   "update ",
		Short: "Update soften topic or topics (only for partitioned).",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs.groundTopic = args[0]
			updateTopics(rtArgs, mdlArgs, cmdArgs)
		},
	}
	// parse variables
	cmd.Flags().UintVarP(&cmdArgs.partitions, "partitions", "p", 0, util.PartitionsUsage4Update)
	cmd.Flags().BoolVarP(&cmdArgs.all, "all", "A", false, util.AllUsage)

	return cmd
}

func updateTopics(rtArgs *internal.RootArgs, mdlArgs *topicsArgs, cmdArgs *updateArgs) {
	if cmdArgs.partitions <= 0 {
		logrus.Fatal("please specify the partitions (with -P or --partitions options) " +
			"and make sure it is more than the original value")
	}
	var topics []string
	var err error
	if cmdArgs.all {
		// query topics from broker
		topics, err = queryTopicsFromBrokerByOptions(queryOptions{
			url:         rtArgs.Url,
			groundTopic: cmdArgs.groundTopic,
			partitioned: true,
		})
		if err == nil && len(topics) == 0 {
			err = errors.New("topic not existed")
		}
		if err != nil {
			logrus.Fatalf("updated \"%s\" failed: %v\n", cmdArgs.groundTopic, err)
		}
	} else {
		// filter by options
		if mdlArgs.level != "" || mdlArgs.status != "" || mdlArgs.subscription != "" {
			matchedTopics := make([]string, 0)
			expectedTopics := util.FormatTopics(cmdArgs.groundTopic, mdlArgs.level, mdlArgs.status, mdlArgs.subscription)
			for _, t := range expectedTopics {
				if slices.Contains(topics, t) {
					matchedTopics = append(matchedTopics, t)
				}
			}
			topics = matchedTopics
		}
	}

	// update partitions
	manager := admin.NewPartitionedTopicManager(rtArgs.Url)
	for _, topic := range topics {
		err := manager.Update(topic, cmdArgs.partitions)
		if err != nil {
			logrus.Fatalf("updated \"%s\" failed: %v\n", topic, err)
		} else {
			logrus.Fatalf("updated \"%s\" successfully, partitions is %v now\n", topic, cmdArgs.partitions)
		}
	}
}
