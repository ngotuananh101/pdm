package ui

import "log/slog"

// defaultLogger adapts slog.Default() to MainWindow's logger interface.
type defaultLogger struct{}

func (defaultLogger) Warn(msg string, args ...any) {
	slog.Default().Warn(msg, args...)
}
