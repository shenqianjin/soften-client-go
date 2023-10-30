package interceptor

import (
	"context"

	"github.com/shenqianjin/soften-client-go/soften/handler"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

// ConsumeInterceptor defines interceptor points during all consuming lifecycle.
type ConsumeInterceptor interface {

	// OnDecide This is called when consumer sends the acknowledgment to the broker.
	OnDecide(ctx context.Context, message message.Message, handleStatus handler.HandleStatus)
}

type ConsumeInterceptors []ConsumeInterceptor

func (x ConsumeInterceptors) OnDecide(ctx context.Context, message message.Message, handleStatus handler.HandleStatus) {
	for i := range x {
		x[i].OnDecide(ctx, message, handleStatus)
	}
}
