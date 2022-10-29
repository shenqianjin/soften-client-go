package topics

import (
	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/spf13/cobra"
)

func NewModuleCmd(rootArgs *internal.RootArgs) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topics ",
		Short: "sub command to manage topics",
	}

	// add action commands
	cmd.AddCommand(newCreateCommand(rootArgs))
	cmd.AddCommand(newDeleteCommand(rootArgs))
	cmd.AddCommand(newUpdateCommand(rootArgs))
	cmd.AddCommand(newListCommand(rootArgs))

	return cmd
}
