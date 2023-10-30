package interceptor

import (
	"context"

	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/handler"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

type decideInterceptFuncMap map[internal.DecideGoto]func(ctx context.Context, msg message.Message)

type decideInterceptor struct {
	interceptFuncMap decideInterceptFuncMap
}

func (i decideInterceptor) OnDecide(ctx context.Context, msg message.Message, handleStatus handler.HandleStatus) {
	if interceptFunc, ok := i.interceptFuncMap[handleStatus.GetGoto()]; ok {
		interceptFunc(ctx, msg)
	}
}

type decideInterceptorBuilder struct {
	interceptFuncMap decideInterceptFuncMap
}

func NewDecideInterceptorBuilder() *decideInterceptorBuilder {
	return &decideInterceptorBuilder{
		interceptFuncMap: make(decideInterceptFuncMap, 0),
	}
}

func (b *decideInterceptorBuilder) OnDecideToDead(f func(ctx context.Context, msg message.Message)) *decideInterceptorBuilder {
	b.interceptFuncMap[decider.GotoDead] = f
	return b
}

func (b *decideInterceptorBuilder) Build() decideInterceptor {
	return decideInterceptor{
		interceptFuncMap: b.interceptFuncMap,
	}
}
