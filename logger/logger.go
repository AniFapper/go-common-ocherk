package logger

import (
	"log/slog"
	"os"
)

// Setup initializes a global structured logger.
// env: "prod" for JSON output, "dev" for readable text output.
func Setup(env string, level slog.Level) {
	var handler slog.Handler

	if env == "prod" {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	} else {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)
}
