package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"key_skew/internal/common"
	"key_skew/internal/jobs"
)

func main() {
	var reduceID int
	var inputsFile string
	var outPath string
	var jobName string

	flag.IntVar(&reduceID, "reduce-id", -1, "Reducer ID")
	flag.StringVar(&inputsFile, "inputs", "", "File containing list of input file paths")
	flag.StringVar(&outPath, "out", "", "Output JSONL file path")
	flag.StringVar(&jobName, "job", "wordcount", "Job name")
	flag.Parse()

	// Initialize logging
	logDir := filepath.Join(filepath.Dir(outPath), "..", "..", "logs")
	if err := common.InitLogging(logDir, fmt.Sprintf("reducer_%d", reduceID)); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logging: %v\n", err)
		os.Exit(1)
	}
	defer common.CloseLogging()

	startTime := time.Now()
	common.LogInfo("REDUCER", "Starting reducer: reduce_id=%d, inputs=%s", reduceID, inputsFile)

	// Read input file list
	inputFiles, err := common.ReadReducerInputs(inputsFile)
	if err != nil {
		common.LogError("REDUCER", "Failed to read inputs file: %v", err)
		os.Exit(1)
	}

	// Group by key in memory
	// Use canonical string representation for grouping, but preserve original key type
	keyGroups := make(map[string][]int)      // canonical key -> values
	keyTypes := make(map[string]interface{}) // canonical key -> original key type

	var recordsRead int64
	var bytesRead int64

	// Stream records from all input files
	for _, inputFile := range inputFiles {
		err := common.StreamJSONL(inputFile, func(record common.KV) error {
			recordsRead++
			// Approximate bytes read (JSON marshaled size)
			if jsonBytes, err := json.Marshal(record); err == nil {
				bytesRead += int64(len(jsonBytes) + 1) // +1 for newline
			}

			// Get canonical key representation
			canonicalKey, err := common.CanonicalKey(record.K)
			if err != nil {
				return fmt.Errorf("failed to canonicalize key: %w", err)
			}

			// Store original key type if not seen before
			if _, exists := keyTypes[canonicalKey]; !exists {
				keyTypes[canonicalKey] = record.K
			}

			// Add value to group
			keyGroups[canonicalKey] = append(keyGroups[canonicalKey], record.V)

			return nil
		})

		if err != nil {
			common.LogError("REDUCER", "Failed to stream from %s: %v", inputFile, err)
			os.Exit(1)
		}
	}

	// Create output directory
	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		common.LogError("REDUCER", "Failed to create output directory: %v", err)
		os.Exit(1)
	}

	// Open output file
	outFile, err := os.Create(outPath)
	if err != nil {
		common.LogError("REDUCER", "Failed to create output file: %v", err)
		os.Exit(1)
	}
	defer outFile.Close()

	encoder := json.NewEncoder(outFile)

	// Reduce each key group
	for canonicalKey, values := range keyGroups {
		// Extract base key (word without salt) for reduce function
		baseKey, err := common.ExtractBaseKey(keyTypes[canonicalKey])
		if err != nil {
			common.LogError("REDUCER", "Failed to extract base key: %v", err)
			os.Exit(1)
		}

		// Apply reduce function
		reducedValue := jobs.ReduceKey(baseKey, values)

		// Write output using original key type
		outputRecord := common.OutputRecord{
			K: keyTypes[canonicalKey],
			V: reducedValue,
		}

		if err := encoder.Encode(outputRecord); err != nil {
			common.LogError("REDUCER", "Failed to encode output record: %v", err)
			os.Exit(1)
		}
	}

	// Write stats
	elapsed := time.Since(startTime)
	stats := map[string]interface{}{
		"records_read": recordsRead,
		"bytes_read":   bytesRead,
		"unique_keys":  len(keyGroups),
		"elapsed_ms":   elapsed.Milliseconds(),
	}

	statsPath := filepath.Join(filepath.Dir(outPath), fmt.Sprintf("reduce_%d.stats.json", reduceID))
	statsFile, err := os.Create(statsPath)
	if err != nil {
		common.LogError("REDUCER", "Failed to create stats file: %v", err)
		os.Exit(1)
	}
	defer statsFile.Close()

	statsEncoder := json.NewEncoder(statsFile)
	if err := statsEncoder.Encode(stats); err != nil {
		common.LogError("REDUCER", "Failed to encode stats: %v", err)
		os.Exit(1)
	}

	common.LogInfo("REDUCER", "Reducer completed: records=%d, keys=%d, elapsed=%dms",
		recordsRead, len(keyGroups), elapsed.Milliseconds())
}
