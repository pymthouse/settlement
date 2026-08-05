// Package logging provides the structured logger shared by both binaries.
//
// Billing logs are read during incident response and audits, so they are JSON
// by default and never carry raw webhook bodies — only identifiers.
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// New builds a JSON logger at the level named by SETTLEMENT_LOG_LEVEL
// (debug|info|warn|error, default info) and tags every record with service.
func New(service string) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(strings.TrimSpace(os.Getenv("SETTLEMENT_LOG_LEVEL"))) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: level}
	if strings.EqualFold(strings.TrimSpace(os.Getenv("SETTLEMENT_LOG_FORMAT")), "text") {
		handler = slog.NewTextHandler(os.Stdout, opts)
	} else {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}
	return slog.New(handler).With("service", service)
}
