package utils

import (
	"context"
	"log"
)

// GetLogger extracts a *log.Logger from the given context. If no logger is
// found, it falls back to the standard logger.
func GetLogger(ctx context.Context) *log.Logger {
	if l, ok := ctx.Value(CtxKeyLogger).(*log.Logger); ok {
		return l
	}
	return log.Default()
}
