package logger

import (
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"sync"
)

var _ Logger = (*FLogger)(nil)

var (
	loggerMu sync.RWMutex
	logger   = Logger(flog)
)

const (
	_flogPrefix = "csqlite "
)

var flog = &FLogger{
	Logger: log.New(os.Stderr, _flogPrefix, log.LstdFlags),
}

// FLogger calldepth
const _calldepth = 2

type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
	Panicf(format string, args ...any)
}

func SetLogger(l Logger) {
	loggerMu.Lock()
	defer loggerMu.Unlock()
	logger = l
}

func ResetDefaultLogger() {
	SetLogger(flog)
}

func GetLogger() Logger {
	loggerMu.RLock()
	defer loggerMu.RUnlock()
	return logger
}

type FLogger struct {
	*log.Logger
	debug bool
}

func (fl *FLogger) EnableDebug() {
	fl.debug = true
}

func (fl *FLogger) Debugf(format string, args ...any) {
	if fl.debug {
		_ = fl.Output(_calldepth, fl.header("DEBUG", fmt.Sprintf(format, args...)))
	}
}

func (fl *FLogger) Infof(format string, args ...any) {
	_ = fl.Output(_calldepth, fl.header("INFO", fmt.Sprintf(format, args...)))
}

func (fl *FLogger) Warnf(format string, args ...any) {
	_ = fl.Output(_calldepth, fl.header("WARN", fmt.Sprintf(format, args...)))
}

func (fl *FLogger) Errorf(format string, args ...any) {
	_ = fl.Output(_calldepth, fl.header("ERROR", fmt.Sprintf(format, args...)))
}

func (fl *FLogger) Fatalf(format string, args ...any) {
	_ = fl.Output(_calldepth, fl.header("FATAL", fmt.Sprintf(format, args...)))
}

func (fl *FLogger) Panicf(format string, args ...any) {
	fl.Logger.Panicf(format, args...)
}

func (fl *FLogger) header(lvl, msg string) string {
	_, file, line, ok := runtime.Caller(_calldepth)
	if !ok {
		file = "unknown"
		line = 0
	} else {
		file = path.Base(file)
	}
	return fmt.Sprintf("%s:%d [%s] %s", file, line, lvl, msg)
}
