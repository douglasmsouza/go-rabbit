package rabbitmq

import (
	"os"

	"github.com/koding/logging"
)

func newLogger(name string, level logging.Level) logging.Logger {
	logHandler := logging.NewWriterHandler(os.Stderr)
	logHandler.SetLevel(level)

	logger := logging.NewLogger(name)
	logger.SetLevel(level)
	logger.SetHandler(logHandler)
	
	return logger
}
