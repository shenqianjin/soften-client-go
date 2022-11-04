package meta

import (
	"context"
)

const KeyReqId = "reqId"

// metasKeyType is private and exclusive meta key type
// DO NOT expose it out of current package
type metasKeyType struct{}

type metas map[any]any

// constant

var metasKey = metasKeyType{}

func AsMetas(ctx context.Context) context.Context {
	if isMetas(ctx) {
		return ctx
	}
	return context.WithValue(ctx, metasKey, make(metas))
}

func GetMeta(ctx context.Context, key any) any {
	if metaMap := getMetas(ctx); metaMap != nil {
		if meta, existed := metaMap[key]; existed {
			return meta
		}
	}
	return nil
}

func PutMeta(ctx context.Context, key any, value any) bool {
	if metaMap := getMetas(ctx); metaMap != nil {
		metaMap[key] = value
		return true
	}
	return false
}

// ------ helpers ------

func getMetas(ctx context.Context) metas {
	if val := ctx.Value(metasKey); val != nil {
		if metaMap, ok := val.(metas); ok {
			return metaMap
		}
	}
	return nil
}

func isMetas(ctx context.Context) bool {
	return getMetas(ctx) != nil
}
