package messages

import (
	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/spf13/cobra"
)

func NewModuleCmd(rootArgs internal.RootArgs) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "messages ",
		Short: "sub command to manage messages",
	}

	// add action commands
	cmd.AddCommand(newRecallCommand(rootArgs))

	return cmd
}
