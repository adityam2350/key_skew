package common

import (
	"bufio"
	"encoding/json"
	"os"
)

// ReadJSONL reads a JSONL file and returns all records
func ReadJSONL(path string) ([]InputRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var records []InputRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var record InputRecord
		if err := json.Unmarshal(line, &record); err != nil {
			continue
		}
		records = append(records, record)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return records, nil
}

// StreamJSONL streams records from a JSONL file calling fn for each record
func StreamJSONL(path string, fn interface{}) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var kv KV
		if err := json.Unmarshal(line, &kv); err == nil {
			if fnKV, ok := fn.(func(KV) error); ok {
				if err := fnKV(kv); err != nil {
					return err
				}
				continue
			}
		}

		var output OutputRecord
		if err := json.Unmarshal(line, &output); err == nil {
			if fnOutput, ok := fn.(func(OutputRecord) error); ok {
				if err := fnOutput(output); err != nil {
					return err
				}
				continue
			}
		}
	}

	return scanner.Err()
}

// ReadReducerInputs reads a file containing a list of input file paths
func ReadReducerInputs(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var paths []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 {
			paths = append(paths, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return paths, nil
}

// NewJSONLScanner creates a scanner for reading JSONL files
func NewJSONLScanner(file *os.File) *bufio.Scanner {
	return bufio.NewScanner(file)
}
