package interceptor

import (
	"context"

	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

type onDecideInterceptFunc func(ctx context.Context, msg message.Message, decision decider.Decision)

type decideInterceptor struct {
	interceptFunc onDecideInterceptFunc
}

func NewDecideInterceptor(interceptFunc onDecideInterceptFunc) decideInterceptor {
	return decideInterceptor{
		interceptFunc: interceptFunc,
	}
}

func (i decideInterceptor) OnDecide(ctx context.Context, msg message.Message, decision decider.Decision) {
	i.interceptFunc(ctx, msg, decision)
}

// ------ add default implementation for other intercept functions ------
