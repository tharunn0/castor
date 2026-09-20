package main

import (
	"fmt"
	"log/slog"
	"os"
)

func main() {

	envs := []string{"prod", "dev", "off"}

	fmt.Println("============ dev =================")
	testdev(envs[1])
	fmt.Println()
	fmt.Println("============ prod =================")
	testprod(envs[0])

	fmt.Println("============ off =================")

	testoff(envs[2])
}

func testdev(env string) {
	log := newLogger(env)
	log.Debug("very important log")
	log.Info("very important log")
	log.Error("very important log")
	log.Warn("very important log")
}
func testprod(env string) {
	log := newLogger(env)
	log.Debug("very important log")
	log.Info("very important log")
	log.Error("very important log")
	log.Warn("very important log")
}
func testoff(env string) {
	log := newLogger(env)
	log.Debug("very important log")
	log.Info("very important log")
	log.Error("very important log")
	log.Warn("very important log")
}

func newLogger(env string) *slog.Logger {

	var logHandler slog.Handler

	switch env {
	case "dev":
		logHandler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			AddSource: true,
			Level:     slog.LevelDebug,
		})
	case "prod":
		logHandler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level:     slog.LevelInfo,
			AddSource: false,
		})

	case "off":
		logHandler = slog.DiscardHandler
	default:
		return nil
	}

	logger := slog.New(logHandler)
	svcLogger := logger.With("service", "data-svc-1")

	return svcLogger
}
