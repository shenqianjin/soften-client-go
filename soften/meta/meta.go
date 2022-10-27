package meta

import (
	"context"
)

const metasKey = "soften_metas"

type metas map[any]any

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
		if metaMap, ok := val.(map[any]any); ok {
			return metaMap
		}
	}
	return nil
}

func isMetas(ctx context.Context) bool {
	return getMetas(ctx) != nil
}
