package meta

import (
	"context"
	"sync"
	"time"
)

var metaKey = "soften_meta"

type Context interface {
	context.Context
	PutMeta(key, value any)
	GetMeta(key any) (value any, ok bool)
}
type contextImpl struct {
	context.Context
}

func NewContext(parent context.Context) Context {
	meta := &sync.Map{}
	return &contextImpl{
		Context: context.WithValue(parent, metaKey, meta),
	}
}

// ------ wrap context.Context methods ------

func (ctx *contextImpl) Deadline() (deadline time.Time, ok bool) {
	return ctx.Context.Deadline()
}

func (ctx *contextImpl) Done() <-chan struct{} {
	return ctx.Context.Done()
}

func (ctx *contextImpl) Err() error {
	return ctx.Context.Err()
}

func (ctx *contextImpl) Value(key any) any {
	return ctx.Context.Value(key)
}

// ------ extra method ------

func (ctx *contextImpl) PutMeta(key, value any) {
	meta := ctx.getMetas()
	meta.Store(key, value)
}

func (ctx *contextImpl) GetMeta(key any) (value any, ok bool) {
	meta := ctx.getMetas()
	return meta.Load(key)
}

func (ctx *contextImpl) getMetas() *sync.Map {
	cv := ctx.Context.Value(metaKey)
	if cv == nil {
		panic("Invalid MetaContext")
	}
	metas, ok := cv.(*sync.Map)
	if !ok || metas == nil {
		panic("Invalid Meta")
	}
	return metas
}
