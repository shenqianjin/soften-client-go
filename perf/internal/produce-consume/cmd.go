package produce_consume

import (
	"sync"

	"github.com/shenqianjin/soften-client-go/perf/internal"
	"github.com/shenqianjin/soften-client-go/perf/internal/consume"
	"github.com/shenqianjin/soften-client-go/perf/internal/produce"
	"github.com/spf13/cobra"
)

func NewProduceConsumeCommand(rtArgs *internal.RootArgs) *cobra.Command {
	pArgs := &produce.ProduceArgs{}
	cArgs := &consume.ConsumeArgs{}
	cmd := &cobra.Command{
		Use:   "produce-consume ",
		Short: "Both produce and consume on a topic and measure performance",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			group := sync.WaitGroup{}
			group.Add(2)
			// produce
			go func() {
				pArgs.Topic = args[0]
				produce.PerfProduce(rtArgs.Ctx, rtArgs, pArgs)
				group.Done()
			}()
			// consume
			go func() {
				cArgs.Topic = args[0]
				consume.PerfConsume(rtArgs.Ctx, rtArgs, cArgs)
				group.Done()
			}()
			group.Wait()
		},
	}
	// load flags here
	produce.LoadProduceFlags(cmd.Flags(), pArgs)
	consume.LoadConsumeFlags(cmd.Flags(), cArgs)
	return cmd
}
