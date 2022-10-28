package meta

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMetaGetPut(t *testing.T) {
	baseCtx := context.Background()
	assert.True(t, nil == getMetas(baseCtx))
	assert.Equal(t, nil, GetMeta(baseCtx, "key1"))
	assert.Equal(t, false, PutMeta(baseCtx, "key1", 12))
	assert.Equal(t, nil, GetMeta(baseCtx, "key1"))

	ctx := AsMetas(baseCtx)
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

func TestMetaParse(t *testing.T) {
	ctx := AsMetas(context.Background())
	p1 := parse1(ctx)
	p2 := parse2(ctx)
	m := parseMsg(ctx)
	fmt.Println("main", m)
	assert.Equal(t, p1, p2)
	assert.Equal(t, p1, m)
}

func parse1(ctx context.Context) map[string]interface{} {
	log := parseMsg(ctx)
	fmt.Println("parse1", log)
	return log
}

func parse2(ctx context.Context) map[string]interface{} {
	log := parseMsg(ctx)
	fmt.Println("parse2", log)
	return log
}

func parseMsg(ctx context.Context) map[string]interface{} {
	key := "msg"
	if m := GetMeta(ctx, key); m != nil {
		if logger, ok3 := m.(map[string]interface{}); ok3 {
			return logger
		}
	}
	xl := map[string]interface{}{
		"A":    1234,
		"Bbb":  "aaaaa",
		"time": time.Now().Format(time.RFC3339Nano),
	}
	PutMeta(ctx, key, xl)
	return xl
}
