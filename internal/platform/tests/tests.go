package tests

import (
	"context"

	"github.com/tokenized/pkg/logger"
)

// Context creates a context for testing.
func Context() context.Context {
	return logger.ContextWithLogConfig(context.Background(), logger.NewConfig(true, true, ""))
}
