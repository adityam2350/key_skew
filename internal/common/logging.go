package common

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

var logFile *os.File
var logFilePath string

// InitLogging initializes logging to a file
func InitLogging(logDir string, component string) error {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	logFilePath = filepath.Join(logDir, fmt.Sprintf("%s.log", component))
	var err error
	logFile, err = os.Create(logFilePath)
	if err != nil {
		return fmt.Errorf("failed to create log file: %w", err)
	}

	return nil
}

// CloseLogging closes the log file
func CloseLogging() error {
	if logFile != nil {
		return logFile.Close()
	}
	return nil
}

// Log writes a log message to both stdout and the log file
func Log(level, component, message string, args ...interface{}) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	formattedMsg := fmt.Sprintf(message, args...)
	logLine := fmt.Sprintf("[%s] [%s] [%s] %s\n", timestamp, level, component, formattedMsg)

	// Write to stdout
	fmt.Print(logLine)

	// Write to log file if initialized
	if logFile != nil {
		if _, err := logFile.WriteString(logLine); err != nil {
			// If we can't write to log file, at least we tried
			_ = err
		}
	}
}

// LogInfo logs an info message
func LogInfo(component, message string, args ...interface{}) {
	Log("INFO", component, message, args...)
}

// LogError logs an error message
func LogError(component, message string, args ...interface{}) {
	Log("ERROR", component, message, args...)
}

// LogWarn logs a warning message
func LogWarn(component, message string, args ...interface{}) {
	Log("WARN", component, message, args...)
}

// LogDebug logs a debug message
func LogDebug(component, message string, args ...interface{}) {
	Log("DEBUG", component, message, args...)
}
