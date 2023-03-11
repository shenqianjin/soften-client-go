package topics

import (
	"time"

	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/namespaces"
	"github.com/shenqianjin/soften-client-go/admin/internal/support/constant"
	"github.com/shenqianjin/soften-client-go/admin/internal/support/util"
	"github.com/shenqianjin/soften-client-go/admin/internal/tenants"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"
)

type createArgs struct {
	groundTopic string
	partitioned bool
	partitions  uint

	autoCreateTenant    bool
	autoCreateNamespace bool
	deadTTL             uint64
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
		Example: "(1) soften-admin topics create test01\n" +
			"(2) soften-admin topics create public/default/test02 -P\n" +
			"(3) soften-admin topics create test03 -Pp 4 -l L1,L2,L3,B1 -s Ready,Pending,Retrying,Dead -S sub\n" +
			"(4) soften-admin topics create persistent://business/finance/equity -Pp 8",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs.groundTopic = args[0]
			createTopics(rtArgs, mdlArgs, cmdArgs)
		},
	}

	// parse partition
	cmd.Flags().BoolVarP(&cmdArgs.partitioned, "partitioned", "P", false, constant.PartitionedUsage)
	cmd.Flags().UintVarP(&cmdArgs.partitions, "partitions", "p", 1, constant.PartitionsUsage4Create)
	cmd.Flags().BoolVar(&cmdArgs.autoCreateTenant, "auto-create-tenant", false, "auto create tenant if missing")
	cmd.Flags().BoolVar(&cmdArgs.autoCreateNamespace, "auto-create-namespace", false, "auto create tenant if missing")

	// dead ttl: default is 3600*24*30*6 (six month)
	cmd.PersistentFlags().Uint64Var(&cmdArgs.deadTTL, "dead-ttl", uint64(3600*24*30*6), constant.DeadTTLUsage)

	return cmd
}

func createTopics(rtArgs *internal.RootArgs, mdlArgs *topicsArgs, cmdArgs *createArgs) {
	namespaceTopic, err := util.ParseNamespaceTopic(cmdArgs.groundTopic)
	if err != nil {
		logrus.Fatal(err)
	}
	if cmdArgs.autoCreateTenant {
		tenants.CreateIfNotExist(rtArgs.WebUrl, namespaceTopic.Tenant)
	}
	if cmdArgs.autoCreateNamespace {
		namespaces.CreateIfNotExist(rtArgs.WebUrl, namespaceTopic.Namespace)
	}
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
	deadTopics := util.FormatTopics(cmdArgs.groundTopic, mdlArgs.level, message.StatusDead.String(), mdlArgs.subscription)
	// create
	manager := admin.NewRobustTopicManager(rtArgs.WebUrl)
	partitions := cmdArgs.partitions
	if !cmdArgs.partitioned {
		partitions = 0 // non-partition
	}
	for _, topic := range topics {
		err1 := manager.Create(topic, partitions)
		if err1 != nil {
			logrus.Warnf("created \"%s\" failed: %v\n", topic, err1)
		} else {
			logrus.Infof("created \"%s\" successfully\n", topic)
		}
		// set extra properties for dead topic: such as set-message-ttl, create subscriptions
		if slices.Contains(deadTopics, topic) {
			setExtraProperties4DeadTopic(rtArgs, mdlArgs, cmdArgs, topic)
		}
	}
}

func setExtraProperties4DeadTopic(rtArgs *internal.RootArgs, mdlArgs *topicsArgs, cmdArgs *createArgs, deadTopic string) {
	manager := admin.NewRobustTopicManager(rtArgs.WebUrl)
	// auto set ttl for dead topic
	err := manager.SetMessageTTL(deadTopic, cmdArgs.deadTTL)
	ttlDuration := time.Duration(cmdArgs.deadTTL) * time.Second
	if err != nil {
		logrus.Warnf("> set message TTL as %v on \"%s\" failed: %v\n", ttlDuration, deadTopic, err)
	} else {
		logrus.Infof("> set message TTL as %v on \"%s\" successfully\n", ttlDuration, deadTopic)
	}

	// auto create subscription for dead topic
	subManager := admin.NewSubscriptionManager(rtArgs.WebUrl)
	// list existed subscriptions
	existedSubs, err1 := subManager.List(deadTopic)
	if err1 != nil {
		logrus.Warnf("> subscribed \"%s\" on\"%s\" failed: %v\n", mdlArgs.subscription, deadTopic, err1)
	}
	subs := util.ParseSubs(mdlArgs.subscription)
	for _, sub := range subs {
		if slices.Contains(existedSubs, sub) {
			continue
		}
		// auto create subscription if current sub is missing on a dead topic
		err2 := subManager.Subscribe(deadTopic, sub)
		if err2 != nil {
			logrus.Warnf("> subscribed \"%s\" on \"%s\" failed: %v\n", sub, deadTopic, err2)
		} else {
			logrus.Infof("> subscribed \"%s\" on \"%s\" successfully\n", sub, deadTopic)
		}
	}
}
