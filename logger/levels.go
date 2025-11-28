package logger

import "log/slog"

// Custom log levels
const (
	LevelTrace slog.Level = -8
	LevelFatal slog.Level = 12
)

// Level names for custom levels
var levelNames = map[slog.Leveler]string{
	LevelTrace: "TRACE",
	LevelFatal: "FATAL",
}

// LevelName returns the name of a log level
func LevelName(level slog.Level) string {
	if name, ok := levelNames[level]; ok {
		return name
	}
	return level.String()
}