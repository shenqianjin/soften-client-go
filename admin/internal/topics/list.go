package topics

import (
	"fmt"
	"sort"
	"strings"

	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/util"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type listArgs struct {
	groundTopic      string
	subscription     string
	partitioned      bool
	groundOnly       bool
	readyOnly        bool
	nonPartitionOnly bool
}

func newListCommand(rtArgs *internal.RootArgs) *cobra.Command {
	cmdArgs := &listArgs{}
	cmd := &cobra.Command{
		Use:   "list ",
		Short: "list soften topic or topics by ground topic or namespace",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs.groundTopic = args[0]
			listTopics(*rtArgs, cmdArgs)
		},
	}

	flags := cmd.Flags()
	// parse partition
	flags.BoolVarP(&cmdArgs.partitioned, "partitioned", "p", false, util.PartitionedUsage)
	flags.StringVarP(&cmdArgs.subscription, "subscription", "S", "", util.SubscriptionUsage)
	flags.BoolVarP(&cmdArgs.groundOnly, "ground-only", "g", false, "list L1 topics only")
	flags.BoolVarP(&cmdArgs.readyOnly, "ready-only", "r", false, "list ready topics only")

	return cmd
}

func listTopics(rtArgs internal.RootArgs, cmdArgs *listArgs) {
	topics, err := listTopicsByOptions(listOptions{
		url:          rtArgs.Url,
		groundTopic:  cmdArgs.groundTopic,
		subscription: cmdArgs.subscription,

		partitioned: cmdArgs.partitioned,
		groundOnly:  cmdArgs.groundOnly,
		readyOnly:   cmdArgs.readyOnly,
	})

	if err != nil {
		logrus.Fatalf("list \"%s\" failed: %v\n", cmdArgs.groundTopic, err)
	} else {
		sort.Strings(topics)
		for _, topic := range topics {
			fmt.Println(topic)
		}
	}
}

type listOptions struct {
	url          string
	groundTopic  string
	subscription string

	partitioned bool
	groundOnly  bool
	readyOnly   bool
}

func listTopicsByOptions(options listOptions) ([]string, error) {
	manager := admin.NewRobustTopicManager(options.url)

	namespace := "public/default"
	// remove schema
	if ti := strings.Index(options.groundTopic, "://"); ti > 0 {
		namespace = options.groundTopic[ti+3:]
	}
	// remove raw topic name
	if strings.Contains(namespace, "/") {
		segments := strings.Split(namespace, "/")
		if len(segments) > 2 {
			namespace = strings.Join(segments[0:1], "/")
		}
	}
	var queriedTopics []string
	var err error
	queriedTopics, err = manager.List(namespace)
	if err != nil {
		logrus.Fatalln(err)
	}

	// match by ground topic (may namespace as well)
	matchedTopics := make([]string, 0)
	for _, topic := range queriedTopics {
		if strings.Contains(topic, options.groundTopic) {
			matchedTopics = append(matchedTopics, topic)
		}
	}

	// exclude non-ground queriedTopics
	if options.groundOnly {
		groundTopics := make([]string, 0)
		for _, topic := range matchedTopics {
			if !util.IsPartitionedSubTopic(topic) && util.IsL1Topic(topic) && util.IsReadyTopic(topic) {
				groundTopics = append(groundTopics, topic)
			}
		}
		matchedTopics = groundTopics
	}

	// exclude non-ready queriedTopics
	if options.readyOnly {
		readyTopics := make([]string, 0)
		for _, topic := range matchedTopics {
			if !util.IsPartitionedSubTopic(topic) && util.IsReadyTopic(topic) {
				readyTopics = append(readyTopics, topic)
			}
		}
		matchedTopics = readyTopics
	}

	return matchedTopics, err
}
