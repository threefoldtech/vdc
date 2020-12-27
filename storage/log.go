package main

import (
	"context"
	"fmt"
)

type contextKey string

// CtxKey for context based logging.
var CtxKey = contextKey("ID")

// ReqID for logging request ID.
var ReqID = contextKey("Req-ID")

//AddContextToLogMessage helps in contextbased logging
func AddContextToLogMessage(ctx context.Context, format string) string {
	id := ctx.Value(CtxKey)
	if id == nil {
		return format
	}
	a := fmt.Sprintf("ID: %v ", id)
	reqID := ctx.Value(ReqID)
	if reqID == nil {
		return a + format
	}
	a += fmt.Sprintf("Req-ID: %v ", reqID)
	return a + format
}
