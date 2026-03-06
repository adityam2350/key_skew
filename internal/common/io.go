package common

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// ReadJSONL reads a JSONL file and returns all InputRecord entries
func ReadJSONL(filepath string) ([]InputRecord, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filepath, err)
	}
	defer file.Close()

	var records []InputRecord
	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var record InputRecord
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, fmt.Errorf("failed to unmarshal line %d in %s: %w", lineNum, filepath, err)
		}
		records = append(records, record)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file %s: %w", filepath, err)
	}

	return records, nil
}

// WriteJSONL writes KV records to a JSONL file
func WriteJSONL(filePath string, records []KV) error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filePath, err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	for _, record := range records {
		line, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("failed to marshal record: %w", err)
		}
		if _, err := writer.Write(line); err != nil {
			return fmt.Errorf("failed to write line: %w", err)
		}
		if _, err := writer.WriteString("\n"); err != nil {
			return fmt.Errorf("failed to write newline: %w", err)
		}
	}

	return nil
}

// AppendJSONL appends a single KV record to a JSONL file
func AppendJSONL(filepath string, record KV) error {
	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filepath, err)
	}
	defer file.Close()

	line, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %w", err)
	}

	writer := bufio.NewWriter(file)
	if _, err := writer.Write(line); err != nil {
		return fmt.Errorf("failed to write line: %w", err)
	}
	if _, err := writer.WriteString("\n"); err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
	}
	return writer.Flush()
}

// ReadReducerInputs reads a file containing a list of input file paths (one per line)
func ReadReducerInputs(inputsFile string) ([]string, error) {
	file, err := os.Open(inputsFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open inputs file %s: %w", inputsFile, err)
	}
	defer file.Close()

	var files []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 {
			files = append(files, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading inputs file %s: %w", inputsFile, err)
	}

	return files, nil
}

// StreamJSONL streams JSONL records from a file, calling callback for each record
// Handles both string and array keys in JSON unmarshaling
func StreamJSONL(filepath string, callback func(record KV) error) error {
	file, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filepath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		// Unmarshal into a map first to handle dynamic key types
		var rawRecord map[string]json.RawMessage
		if err := json.Unmarshal(line, &rawRecord); err != nil {
			return fmt.Errorf("failed to unmarshal line %d in %s: %w", lineNum, filepath, err)
		}

		// Extract key
		keyJSON, ok := rawRecord["k"]
		if !ok {
			return fmt.Errorf("missing 'k' field in line %d", lineNum)
		}

		key, err := UnmarshalKey(keyJSON)
		if err != nil {
			return fmt.Errorf("failed to unmarshal key in line %d: %w", lineNum, err)
		}

		// Extract value (can be int or string depending on job type)
		valueJSON, ok := rawRecord["v"]
		if !ok {
			return fmt.Errorf("missing 'v' field in line %d", lineNum)
		}

		// Try to unmarshal as int first (for WordCount), then as string (for PageRank)
		var value interface{}
		var intValue int
		if err := json.Unmarshal(valueJSON, &intValue); err == nil {
			value = intValue
		} else {
			// Try as string (for PageRank JSON values)
			var strValue string
			if err := json.Unmarshal(valueJSON, &strValue); err == nil {
				value = strValue
			} else {
				return fmt.Errorf("failed to unmarshal value in line %d: %w", lineNum, err)
			}
		}

		record := KV{K: key, V: value}
		if err := callback(record); err != nil {
			return fmt.Errorf("callback error for line %d: %w", lineNum, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file %s: %w", filepath, err)
	}

	return nil
}

// WriteReducerInputs writes a list of file paths to an inputs file
func WriteReducerInputs(inputsFile string, files []string) error {
	// Create directory if needed
	dir := filepath.Dir(inputsFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	file, err := os.Create(inputsFile)
	if err != nil {
		return fmt.Errorf("failed to create inputs file %s: %w", inputsFile, err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	for _, f := range files {
		if _, err := writer.WriteString(f + "\n"); err != nil {
			return fmt.Errorf("failed to write file path: %w", err)
		}
	}

	return nil
}
