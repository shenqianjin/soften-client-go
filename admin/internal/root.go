package internal

import (
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/spf13/cobra"
)

type rootArgs struct {
	debug bool
	url   string
}

func NewRootCmd() (*cobra.Command, rootArgs) {
	cmdArgs := rootArgs{}
	cmd := &cobra.Command{
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			config.DebugMode = cmdArgs.debug
		},
		Use: "soften-admin",
	}

	// parser variables
	flags := cmd.PersistentFlags()
	flags.BoolVarP(&cmdArgs.debug, "debug", "d", false, "enable debug mode")
	flags.StringVarP(&cmdArgs.url, "url", "u", "http://localhost:8080", "pulsar broker http url")
	return cmd, cmdArgs
}
