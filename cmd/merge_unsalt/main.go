package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"key_skew/internal/common"
)

func main() {
	var inputsGlob string
	var outPath string

	flag.StringVar(&inputsGlob, "inputs-glob", "", "Glob pattern for reducer output files")
	flag.StringVar(&outPath, "out", "", "Output JSONL file path")
	flag.Parse()

	// Initialize logging
	logDir := filepath.Join(filepath.Dir(outPath), "..", "logs")
	if err := common.InitLogging(logDir, "merge_unsalt"); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logging: %v\n", err)
		os.Exit(1)
	}
	defer common.CloseLogging()

	startTime := time.Now()
	common.LogInfo("MERGE_UNSALT", "Starting merge: glob=%s, out=%s", inputsGlob, outPath)

	// Find all files matching the glob pattern
	matches, err := filepath.Glob(inputsGlob)
	if err != nil {
		common.LogError("MERGE_UNSALT", "Failed to glob pattern: %v", err)
		os.Exit(1)
	}

	if len(matches) == 0 {
		common.LogWarn("MERGE_UNSALT", "No files matched glob pattern: %s", inputsGlob)
	}

	// Aggregate values by base key
	aggregated := make(map[string]int)
	var recordsRead int64

	// Read all reducer outputs
	for _, inputFile := range matches {
		err := common.StreamJSONL(inputFile, func(record common.KV) error {
			recordsRead++

			// Extract base key (remove salt if present)
			baseKey, err := common.ExtractBaseKey(record.K)
			if err != nil {
				return fmt.Errorf("failed to extract base key: %w", err)
			}

			// Aggregate values
			aggregated[baseKey] += record.V

			return nil
		})

		if err != nil {
			common.LogError("MERGE_UNSALT", "Failed to stream from %s: %v", inputFile, err)
			os.Exit(1)
		}
	}

	// Create output directory
	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		common.LogError("MERGE_UNSALT", "Failed to create output directory: %v", err)
		os.Exit(1)
	}

	// Write final output with only unsalted keys
	outFile, err := os.Create(outPath)
	if err != nil {
		common.LogError("MERGE_UNSALT", "Failed to create output file: %v", err)
		os.Exit(1)
	}
	defer outFile.Close()

	encoder := json.NewEncoder(outFile)
	for baseKey, totalValue := range aggregated {
		outputRecord := common.OutputRecord{
			K: baseKey, // Unsalted key (string)
			V: totalValue,
		}

		if err := encoder.Encode(outputRecord); err != nil {
			common.LogError("MERGE_UNSALT", "Failed to encode output record: %v", err)
			os.Exit(1)
		}
	}

	elapsed := time.Since(startTime)
	common.LogInfo("MERGE_UNSALT", "Merge completed: records_read=%d, unique_keys=%d, elapsed=%dms",
		recordsRead, len(aggregated), elapsed.Milliseconds())
}
