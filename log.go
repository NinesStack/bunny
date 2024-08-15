package bunny
// Adapted from github.com/InVisionApp/go-logger

// Logger interface that allows a logging library of choice to be used with Bunny
type Logger interface {
	Debug(msg ...interface{})
	Info(msg ...interface{})
	Warn(msg ...interface{})
	Error(msg ...interface{})

	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})

	Debugln(msg ...interface{})
	Infoln(msg ...interface{})
	Warnln(msg ...interface{})
	Errorln(msg ...interface{})
}

/*************
 No-Op Logger
*************/

type noop struct{}

// NewNoopLogger creates a no-op logger that can be used to silence
// all logging from this library. Also useful in tests.
func NewNoopLogger() Logger {
	return &noop{}
}

// Debug log message no-op
func (n *noop) Debug(msg ...interface{}) {}

// Info log message no-op
func (n *noop) Info(msg ...interface{}) {}

// Warn log message no-op
func (n *noop) Warn(msg ...interface{}) {}

// Error log message no-op
func (n *noop) Error(msg ...interface{}) {}

// Debugln line log message no-op
func (n *noop) Debugln(msg ...interface{}) {}

// Infoln line log message no-op
func (n *noop) Infoln(msg ...interface{}) {}

// Warnln line log message no-op
func (n *noop) Warnln(msg ...interface{}) {}

// Errorln line log message no-op
func (n *noop) Errorln(msg ...interface{}) {}

// Debugf log message with formatting no-op
func (n *noop) Debugf(format string, args ...interface{}) {}

// Infof log message with formatting no-op
func (n *noop) Infof(format string, args ...interface{}) {}

// Warnf log message with formatting no-op
func (n *noop) Warnf(format string, args ...interface{}) {}

// Errorf log message with formatting no-op
func (n *noop) Errorf(format string, args ...interface{}) {}
