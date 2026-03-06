package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"key_skew/common/common"
	"key_skew/common/jobs"
	// Import master packages to trigger job registration via their init() functions
	_ "key_skew/wordcount/master"
	_ "key_skew/pagerank/master"
)

func main() {
	var mode string
	var mapID int
	var shardPath string
	var outDir string
	var sampleRate float64
	var R int
	var jobName string
	var planPath string
	var seed int64

	flag.StringVar(&mode, "mode", "", "Mode: 'sample' or 'execute'")
	flag.IntVar(&mapID, "map-id", -1, "Mapper ID")
	flag.StringVar(&shardPath, "shard", "", "Input shard file path")
	flag.StringVar(&outDir, "outdir", "", "Output directory")
	flag.Float64Var(&sampleRate, "sample-rate", 0.01, "Sampling rate (sample mode only)")
	flag.IntVar(&R, "R", 0, "Number of reducers (execute mode only)")
	flag.StringVar(&jobName, "job", "wordcount", "Job name")
	flag.StringVar(&planPath, "plan", "", "Partition plan file path (optional)")
	flag.Int64Var(&seed, "seed", 0, "Global seed for sampling")
	flag.Parse()

	// Initialize logging
	logDir := filepath.Join(outDir, "..", "..", "logs")
	if err := common.InitLogging(logDir, fmt.Sprintf("mapper_%d", mapID)); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logging: %v\n", err)
		os.Exit(1)
	}
	defer common.CloseLogging()

	if mode == "sample" {
		runSampleMode(mapID, shardPath, outDir, sampleRate, seed, jobName)
	} else if mode == "execute" {
		runExecuteMode(mapID, shardPath, outDir, R, planPath, jobName)
	} else {
		common.LogError("MAPPER", "Invalid mode: %s (must be 'sample' or 'execute')", mode)
		os.Exit(1)
	}
}

func runSampleMode(mapID int, shardPath string, outDir string, sampleRate float64, seed int64, jobName string) {
	startTime := time.Now()
	common.LogInfo("MAPPER", "Starting sample mode: map_id=%d, shard=%s", mapID, shardPath)

	// Read shard
	records, err := common.ReadJSONL(shardPath)
	if err != nil {
		common.LogError("MAPPER", "Failed to read shard: %v", err)
		os.Exit(1)
	}

	// Initialize random number generator with seed derived from global seed + mapID
	rng := rand.New(rand.NewSource(seed + int64(mapID)))

	// Sample and aggregate counts
	sampleCounts := make(map[string]int)
	sampledRecords := 0

	// Get job from registry
	registry := jobs.NewJobRegistry()
	job, ok := registry.Get(jobName)
	if !ok {
		common.LogError("MAPPER", "Unknown job: %s", jobName)
		os.Exit(1)
	}

	for _, record := range records {
		kvs := job.Map(record)
		for _, kv := range kvs {
			key, ok := kv.K.(string)
			if !ok {
				continue
			}
			// Only sample integer values (WordCount), skip PageRank values
			if job.ValueType() == "int" && rng.Float64() < sampleRate {
				sampleCounts[key]++
				sampledRecords++
			}
		}
	}

	// Write sample_counts.json
	if err := os.MkdirAll(outDir, 0755); err != nil {
		common.LogError("MAPPER", "Failed to create output directory: %v", err)
		os.Exit(1)
	}

	sampleCountsPath := filepath.Join(outDir, "sample_counts.json")
	file, err := os.Create(sampleCountsPath)
	if err != nil {
		common.LogError("MAPPER", "Failed to create sample_counts.json: %v", err)
		os.Exit(1)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(sampleCounts); err != nil {
		common.LogError("MAPPER", "Failed to encode sample counts: %v", err)
		os.Exit(1)
	}

	// Write map_stats.json
	elapsed := time.Since(startTime)
	stats := map[string]interface{}{
		"records_processed": len(records),
		"sampled_records":   sampledRecords,
		"elapsed_ms":        elapsed.Milliseconds(),
	}

	statsPath := filepath.Join(outDir, "map_stats.json")
	statsFile, err := os.Create(statsPath)
	if err != nil {
		common.LogError("MAPPER", "Failed to create map_stats.json: %v", err)
		os.Exit(1)
	}
	defer statsFile.Close()

	statsEncoder := json.NewEncoder(statsFile)
	if err := statsEncoder.Encode(stats); err != nil {
		common.LogError("MAPPER", "Failed to encode stats: %v", err)
		os.Exit(1)
	}

	common.LogInfo("MAPPER", "Sample mode completed: processed=%d, sampled=%d, elapsed=%dms",
		len(records), sampledRecords, elapsed.Milliseconds())
}

func runExecuteMode(mapID int, shardPath string, outDir string, R int, planPath string, jobName string) {
	startTime := time.Now()
	common.LogInfo("MAPPER", "Starting execute mode: map_id=%d, shard=%s, R=%d, job=%s", mapID, shardPath, R, jobName)

	// Load partition plan if provided
	plan := &common.PartitionPlan{
		R:     R,
		Heavy: make(map[string]common.HeavyKeyInfo),
	}

	if planPath != "" {
		planFile, err := os.Open(planPath)
		if err != nil {
			common.LogError("MAPPER", "Failed to open plan file: %v", err)
			os.Exit(1)
		}
		defer planFile.Close()

		decoder := json.NewDecoder(planFile)
		if err := decoder.Decode(plan); err != nil {
			common.LogError("MAPPER", "Failed to decode plan: %v", err)
			os.Exit(1)
		}
	}

	// Create output directory
	if err := os.MkdirAll(outDir, 0755); err != nil {
		common.LogError("MAPPER", "Failed to create output directory: %v", err)
		os.Exit(1)
	}

	// Open R partition files
	partitionFiles := make([]*os.File, R)
	bytesWritten := make(map[string]int64)

	for r := 0; r < R; r++ {
		partPath := filepath.Join(outDir, fmt.Sprintf("part_%03d.jsonl", r))
		file, err := os.Create(partPath)
		if err != nil {
			common.LogError("MAPPER", "Failed to create partition file %s: %v", partPath, err)
			os.Exit(1)
		}
		partitionFiles[r] = file
		bytesWritten[fmt.Sprintf("part_%03d.jsonl", r)] = 0
	}

	// Ensure files are closed
	defer func() {
		for _, f := range partitionFiles {
			if f != nil {
				f.Close()
			}
		}
	}()

	// Read shard and process
	records, err := common.ReadJSONL(shardPath)
	if err != nil {
		common.LogError("MAPPER", "Failed to read shard: %v", err)
		os.Exit(1)
	}

	// Maintain round-robin salt counters per key
	nextSalt := make(map[string]int)
	kvEmitted := 0

	// Get job from registry
	registry := jobs.NewJobRegistry()
	job, ok := registry.Get(jobName)
	if !ok {
		common.LogError("MAPPER", "Unknown job: %s", jobName)
		os.Exit(1)
	}

	for _, record := range records {
		kvs := job.Map(record)
		for _, kv := range kvs {
			key, ok := kv.K.(string)
			if !ok {
				continue
			}

			var outKey interface{}
			// Apply skew mitigation if job supports it
			if job.SupportsSkewMitigation() {
				if heavyInfo, isHeavy := plan.Heavy[key]; isHeavy {
					// Salt the key
					salt := nextSalt[key] % heavyInfo.Splits
					outKey = common.CreateSaltedKey(key, salt)
					nextSalt[key]++
				} else {
					// Unsalted key
					outKey = key
				}
			} else {
				// Job doesn't support skew mitigation (e.g., PageRank)
				outKey = key
			}

			// Compute partition
			rid, err := common.PartitionKey(outKey, R)
			if err != nil {
				common.LogError("MAPPER", "Failed to partition key: %v", err)
				os.Exit(1)
			}

			// Write to partition file
			outputKV := common.KV{K: outKey, V: kv.V}

			// Marshal to get byte count
			jsonBytes, err := json.Marshal(outputKV)
			if err != nil {
				common.LogError("MAPPER", "Failed to marshal KV: %v", err)
				os.Exit(1)
			}

			// Write JSON + newline
			if _, err := partitionFiles[rid].Write(jsonBytes); err != nil {
				common.LogError("MAPPER", "Failed to write to partition: %v", err)
				os.Exit(1)
			}
			if _, err := partitionFiles[rid].WriteString("\n"); err != nil {
				common.LogError("MAPPER", "Failed to write newline: %v", err)
				os.Exit(1)
			}

			// Count bytes written
			partName := fmt.Sprintf("part_%03d.jsonl", rid)
			bytesWritten[partName] += int64(len(jsonBytes) + 1) // +1 for newline

			kvEmitted++
		}
	}

	// Flush all files
	for _, f := range partitionFiles {
		if f != nil {
			f.Sync()
		}
	}

	// Write map_stats.json
	elapsed := time.Since(startTime)
	stats := map[string]interface{}{
		"records_processed":      len(records),
		"kv_emitted":             kvEmitted,
		"bytes_written_per_part": bytesWritten,
		"elapsed_ms":             elapsed.Milliseconds(),
	}

	statsPath := filepath.Join(outDir, "map_stats.json")
	statsFile, err := os.Create(statsPath)
	if err != nil {
		common.LogError("MAPPER", "Failed to create map_stats.json: %v", err)
		os.Exit(1)
	}
	defer statsFile.Close()

	statsEncoder := json.NewEncoder(statsFile)
	if err := statsEncoder.Encode(stats); err != nil {
		common.LogError("MAPPER", "Failed to encode stats: %v", err)
		os.Exit(1)
	}

	common.LogInfo("MAPPER", "Execute mode completed: processed=%d, emitted=%d, elapsed=%dms",
		len(records), kvEmitted, elapsed.Milliseconds())
}
