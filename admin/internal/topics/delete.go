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

type deleteArgs struct {
	groundTopic string
	partitioned bool
	all         bool
}

func newDeleteCommand(rtArgs *internal.RootArgs, mdlArgs *topicsArgs) *cobra.Command {
	cmdArgs := &deleteArgs{}
	cmd := &cobra.Command{
		Use:   "delete ",
		Short: "Delete soften topic or topics.",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs.groundTopic = args[0]
			deleteTopics(rtArgs, mdlArgs, cmdArgs)
		},
	}
	// parse variables
	cmd.Flags().BoolVarP(&cmdArgs.partitioned, "partitioned", "P", false, util.PartitionedUsage)
	cmd.Flags().BoolVarP(&cmdArgs.all, "all", "A", false, util.AllUsage)

	return cmd
}

func deleteTopics(rtArgs *internal.RootArgs, mdlArgs *topicsArgs, cmdArgs *deleteArgs) {
	var topics []string
	var err error
	if cmdArgs.all {
		// query topics from broker
		topics, err = queryTopicsFromBrokerByOptions(queryOptions{
			url:         rtArgs.Url,
			groundTopic: cmdArgs.groundTopic,
			partitioned: cmdArgs.partitioned,
		})
		if err == nil && len(topics) == 0 {
			err = errors.New("topic not existed")
		}
		if err != nil {
			logrus.Fatalf("delete \"%s\" failed: %v\n", cmdArgs.groundTopic, err)
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
	if err != nil {
		logrus.Fatalf("delete \"%s\" failed: %v\n", cmdArgs.groundTopic, err)
	}

	// delete one by one
	manager := admin.NewRobustTopicManager(rtArgs.Url)
	for _, topic := range topics {
		var err error
		err = manager.Delete(topic)
		if err != nil {
			logrus.Fatalf("deleted \"%s\" failed: %v\n", topic, err)
		} else {
			logrus.Infof("deleted \"%s\" successfully\n", topic)
		}
	}
}
