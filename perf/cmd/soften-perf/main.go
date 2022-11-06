package main

import (
	"fmt"
	"os"

	"github.com/shenqianjin/soften-client-go/perf/internal"
	"github.com/shenqianjin/soften-client-go/perf/internal/consume"
	"github.com/shenqianjin/soften-client-go/perf/internal/produce"
	produceConsume "github.com/shenqianjin/soften-client-go/perf/internal/produce-consume"
)

func main() {
	// init commands
	cmd, rtArgs := internal.NewRootCommand()
	cmd.AddCommand(produce.NewProducerCommand(rtArgs))
	cmd.AddCommand(consume.NewConsumerCommand(rtArgs))
	cmd.AddCommand(produceConsume.NewProduceConsumeCommand(rtArgs))

	// start execute
	if err := cmd.Execute(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "executing command error=%+v\n", err)
		os.Exit(1)
	}
}
