package common

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

var (
	logFile *os.File
	logger  *log.Logger
)

// InitLogging initializes logging to a file
func InitLogging(logDir, prefix string) error {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	logPath := filepath.Join(logDir, fmt.Sprintf("%s.log", prefix))
	var err error
	logFile, err = os.Create(logPath)
	if err != nil {
		return fmt.Errorf("failed to create log file: %w", err)
	}

	logger = log.New(logFile, "", log.LstdFlags)
	return nil
}

// CloseLogging closes the log file
func CloseLogging() {
	if logFile != nil {
		logFile.Close()
		logFile = nil
		logger = nil
	}
}

// LogInfo logs an info message
func LogInfo(component, format string, args ...interface{}) {
	if logger != nil {
		logger.Printf("[%s] INFO: %s", component, fmt.Sprintf(format, args...))
	}
}

// LogError logs an error message
func LogError(component, format string, args ...interface{}) {
	if logger != nil {
		logger.Printf("[%s] ERROR: %s", component, fmt.Sprintf(format, args...))
	}
}

// LogWarn logs a warning message
func LogWarn(component, format string, args ...interface{}) {
	if logger != nil {
		logger.Printf("[%s] WARN: %s", component, fmt.Sprintf(format, args...))
	}
}

// LogDebug logs a debug message
func LogDebug(component, format string, args ...interface{}) {
	if logger != nil {
		logger.Printf("[%s] DEBUG: %s", component, fmt.Sprintf(format, args...))
	}
}
