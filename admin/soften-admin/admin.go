package main

import (
	"fmt"
	"os"

	"github.com/shenqianjin/soften-client-go/admin/internal"
)

func main() {
	// root commands
	rootCmd, rootArgs := internal.NewRootCmd()

	// add commands
	rootCmd.AddCommand(internal.NewCreateCommand(rootArgs))
	rootCmd.AddCommand(internal.NewDeleteCommand(rootArgs))
	rootCmd.AddCommand(internal.NewUpdateCommand(rootArgs))
	rootCmd.AddCommand(internal.NewListCommand(rootArgs))

	// execute
	err := rootCmd.Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "executing command error=%+v\n", err)
		os.Exit(1)
	}
}
