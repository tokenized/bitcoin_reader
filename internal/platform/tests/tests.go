package tests

import (
	"context"

	"github.com/tokenized/logger"
)

// Context creates a context for testing.
func Context() context.Context {
	return logger.ContextWithLogConfig(context.Background(), logger.NewConfig(true, true, ""))
}
