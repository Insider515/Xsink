// logging.go — конструктор slog.Logger із урахуванням sink (journal|stdout)
// і рівня.
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// New — повертає *slog.Logger.
// sink=journal — JSON без часу (systemd додає timestamp сам).
// sink=stdout  — текст із часом.
func New(level, sink string) *slog.Logger {
	lvl := slog.LevelInfo
	switch strings.ToUpper(level) {
	case "DEBUG":
		lvl = slog.LevelDebug
	case "WARN":
		lvl = slog.LevelWarn
	case "ERROR":
		lvl = slog.LevelError
	}
	var h slog.Handler
	switch strings.ToLower(sink) {
	case "journal":
		h = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl, ReplaceAttr: dropTime})
	default:
		h = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	}
	return slog.New(h)
}

// dropTime — systemd-journal сам штампує час.
func dropTime(_ []string, a slog.Attr) slog.Attr {
	if a.Key == slog.TimeKey {
		return slog.Attr{}
	}
	return a
}
