package observability

import (
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// InitZerolog configures global JSON structured logging defaults.
func InitZerolog(serviceName, level string) zerolog.Logger {
	zerolog.TimeFieldFormat = time.RFC3339Nano

	lvl := zerolog.InfoLevel
	if parsed, err := zerolog.ParseLevel(strings.ToLower(strings.TrimSpace(level))); err == nil {
		lvl = parsed
	}

	log := zerolog.New(os.Stdout).With().
		Timestamp().
		Str("service", serviceName).
		Logger().
		Level(lvl)

	zerolog.DefaultContextLogger = &log

	return log
}
