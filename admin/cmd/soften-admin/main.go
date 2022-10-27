package main

import (
	"fmt"
	"os"

	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/messages"
	"github.com/shenqianjin/soften-client-go/admin/internal/topics"
)

func main() {
	// root commands
	rootCmd, rootArgs := internal.NewRootCmd()

	// add module commands
	rootCmd.AddCommand(topics.NewModuleCmd(rootArgs))
	rootCmd.AddCommand(messages.NewModuleCmd(rootArgs))

	// execute
	err := rootCmd.Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "executing command error=%+v\n", err)
		os.Exit(1)
	}
}
