package topics

import (
	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/util"
	"github.com/spf13/cobra"
)

type topicsArgs struct {
	status       string
	level        string
	subscription string
}

func NewModuleCmd(rootArgs *internal.RootArgs) *cobra.Command {
	mdlArgs := &topicsArgs{}
	cmd := &cobra.Command{
		Use:   "topics ",
		Short: "Manage topics such as create, delete, update (only for partitioned topics) and list.",
	}

	// add action commands
	cmd.AddCommand(newCreateCommand(rootArgs, mdlArgs))
	cmd.AddCommand(newDeleteCommand(rootArgs, mdlArgs))
	cmd.AddCommand(newUpdateCommand(rootArgs, mdlArgs))
	cmd.AddCommand(newListCommand(rootArgs, mdlArgs))
	cmd.AddCommand(newStatCommand(rootArgs, mdlArgs))

	cmd.PersistentFlags().StringVarP(&mdlArgs.level, "level", "l", "L1", util.LevelUsage)
	cmd.PersistentFlags().StringVarP(&mdlArgs.status, "status", "s", "Ready", util.StatusUsage)
	cmd.PersistentFlags().StringVarP(&mdlArgs.subscription, "subscription", "S", "", util.SubscriptionUsage)

	return cmd
}
