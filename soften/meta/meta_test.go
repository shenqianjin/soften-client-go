package meta

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAbc(t *testing.T) {
	baseCtx := context.Background()
	var nilMetas map[any]any
	initMetas := getMetas(baseCtx)
	fmt.Println(initMetas == nil)
	assert.True(t, nil == getMetas(baseCtx))
	assert.Equal(t, nilMetas, getMetas(baseCtx))
	assert.Equal(t, nil, GetMeta(baseCtx, "key1"))
	assert.Equal(t, false, PutMeta(baseCtx, "key1", 12))
	assert.Equal(t, nil, GetMeta(baseCtx, "key1"))

	ctx := context.WithValue(baseCtx, metasKey, make(map[any]any, 0))
	assert.NotNil(t, getMetas(ctx))
	assert.Equal(t, nil, GetMeta(ctx, "key1"))
	assert.Equal(t, true, PutMeta(ctx, "key1", 12))
	assert.Equal(t, 12, GetMeta(ctx, "key1"))

	subCtx, _ := context.WithCancel(context.WithValue(ctx, "K", "V"))
	assert.NotNil(t, getMetas(subCtx))
	assert.Equal(t, 12, GetMeta(subCtx, "key1"))
	assert.Equal(t, true, PutMeta(subCtx, "key1", 13))
	assert.Equal(t, 13, GetMeta(subCtx, "key1"))
}
