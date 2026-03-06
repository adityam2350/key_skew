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
	var damping float64
	var numNodes int

	flag.IntVar(&reduceID, "reduce-id", -1, "Reducer ID")
	flag.StringVar(&inputsFile, "inputs", "", "File containing list of input file paths")
	flag.StringVar(&outPath, "out", "", "Output JSONL file path")
	flag.StringVar(&jobName, "job", "wordcount", "Job name")
	flag.Float64Var(&damping, "damping", 0.85, "PageRank damping factor (PageRank only)")
	flag.IntVar(&numNodes, "num-nodes", 0, "Number of nodes in graph (PageRank only)")
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

	// Get job from registry
	registry := jobs.NewJobRegistry()
	job, ok := registry.Get(jobName)
	if !ok {
		common.LogError("REDUCER", "Unknown job: %s", jobName)
		os.Exit(1)
	}

	// Set job parameters from command-line flags
	jobParams := make(map[string]interface{})
	if damping > 0 {
		jobParams["damping"] = damping
	}
	if numNodes > 0 {
		jobParams["num_nodes"] = numNodes
	}
	if err := job.SetParameters(jobParams); err != nil {
		common.LogError("REDUCER", "Failed to set job parameters: %v", err)
		os.Exit(1)
	}

	// Group by key in memory
	// Use canonical string representation for grouping, but preserve original key type
	keyGroups := make(map[string]interface{}) // canonical key -> values (type depends on job)
	keyTypes := make(map[string]interface{})  // canonical key -> original key type

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

			// Add value to group based on job type
			if job.ValueType() == "string" {
				// PageRank: values are JSON strings
				if strValue, ok := record.V.(string); ok {
					group, exists := keyGroups[canonicalKey]
					if !exists {
						keyGroups[canonicalKey] = []string{strValue}
					} else if strGroup, ok := group.([]string); ok {
						keyGroups[canonicalKey] = append(strGroup, strValue)
					}
				}
			} else {
				// WordCount: values are integers
				if intValue, ok := record.V.(int); ok {
					group, exists := keyGroups[canonicalKey]
					if !exists {
						keyGroups[canonicalKey] = []int{intValue}
					} else if intGroup, ok := group.([]int); ok {
						keyGroups[canonicalKey] = append(intGroup, intValue)
					}
				}
			}

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

	// Create encoder once for KV format jobs
	encoder := json.NewEncoder(outFile)

	// Reduce each key group using the job interface
	for canonicalKey, values := range keyGroups {
		// Extract base key (word without salt) for reduce function
		baseKey, err := common.ExtractBaseKey(keyTypes[canonicalKey])
		if err != nil {
			common.LogError("REDUCER", "Failed to extract base key: %v", err)
			os.Exit(1)
		}

		// Apply reduce function
		result := job.Reduce(baseKey, values)

		// Write output based on job's output format
		if job.OutputFormat() == "raw" {
			// PageRank: write raw JSON string
			if resultStr, ok := result.(string); ok {
				if _, err := outFile.WriteString(resultStr + "\n"); err != nil {
					common.LogError("REDUCER", "Failed to write output record: %v", err)
					os.Exit(1)
				}
			}
		} else {
			// WordCount: write KV record
			outputRecord := common.OutputRecord{
				K: keyTypes[canonicalKey],
				V: result,
			}

			if err := encoder.Encode(outputRecord); err != nil {
				common.LogError("REDUCER", "Failed to encode output record: %v", err)
				os.Exit(1)
			}
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
