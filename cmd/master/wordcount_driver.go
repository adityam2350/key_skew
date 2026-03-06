package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"key_skew/internal/common"
)

// RunConfig holds configuration for WordCount MapReduce execution
type RunConfig struct {
	InputPath       string
	RunDir          string
	M               int
	R               int
	MaxParallelMaps int
	Mode            string
	SampleRate      float64
	HeavyTopPct     float64
	FixedSplits     int
	Seed            int64
}

// RunSummary holds metrics and summary information for a WordCount run
type RunSummary struct {
	Mode           string                    `json:"mode"`
	M              int                       `json:"M"`
	R              int                       `json:"R"`
	TimesMs        map[string]int64          `json:"times_ms"`
	ReducerLoad    common.ReducerLoadMetrics `json:"reducer_load"`
	CoV            map[string]float64        `json:"cov"`
	MaxMedianRatio map[string]float64        `json:"max_median_ratio"`
	HeavyHitters   map[string]interface{}    `json:"heavy_hitters"`
}

// RunWordCount is the main entry point for WordCount MapReduce execution
func RunWordCount() {
	runFlags := flag.NewFlagSet("run", flag.ExitOnError)
	var config RunConfig
	runFlags.StringVar(&config.InputPath, "input", "", "Input JSONL file path")
	runFlags.StringVar(&config.RunDir, "run-dir", "runs", "Base directory for runs")
	runFlags.IntVar(&config.M, "M", 8, "Number of mappers")
	runFlags.IntVar(&config.R, "R", 8, "Number of reducers")
	runFlags.IntVar(&config.MaxParallelMaps, "max-parallel-maps", 8, "Maximum parallel mappers")
	runFlags.StringVar(&config.Mode, "mode", "baseline", "Mode: baseline or mitigation")
	runFlags.Float64Var(&config.SampleRate, "sample-rate", 0.01, "Sampling rate (mitigation only)")
	runFlags.Float64Var(&config.HeavyTopPct, "heavy-top-pct", 0.01, "Top percentage for heavy hitters (mitigation only)")
	runFlags.IntVar(&config.FixedSplits, "fixed-splits", 8, "Fixed number of splits for heavy keys (mitigation only)")
	runFlags.Int64Var(&config.Seed, "seed", 0, "Global seed")
	runFlags.Parse(os.Args[2:]) // Parse flags after "run"

	// Initialize logging
	if err := common.InitLogging(".", "master"); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logging: %v\n", err)
		os.Exit(1)
	}
	defer common.CloseLogging()

	common.LogInfo("MASTER", "Starting MapReduce run: mode=%s, M=%d, R=%d", config.Mode, config.M, config.R)

	// Create run directory
	timestamp := time.Now().Format("20060102_150405")
	runPath := filepath.Join(config.RunDir, fmt.Sprintf("run_%s_%s", config.Mode, timestamp))
	if err := createRunDirectories(runPath); err != nil {
		common.LogError("MASTER", "Failed to create run directories: %v", err)
		os.Exit(1)
	}

	// Update log directory to run directory
	common.CloseLogging()
	if err := common.InitLogging(filepath.Join(runPath, "logs"), "master"); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logging: %v\n", err)
		os.Exit(1)
	}

	startTime := time.Now()
	var summary RunSummary
	summary.Mode = config.Mode
	summary.M = config.M
	summary.R = config.R
	summary.TimesMs = make(map[string]int64)

	// Shard input
	common.LogInfo("MASTER", "Sharding input into %d shards", config.M)
	if err := shardInput(config.InputPath, runPath, config.M); err != nil {
		common.LogError("MASTER", "Failed to shard input: %v", err)
		os.Exit(1)
	}

	if config.Mode == "baseline" {
		runBaseline(config, runPath, &summary)
	} else if config.Mode == "mitigation" {
		runMitigation(config, runPath, &summary)
	} else {
		common.LogError("MASTER", "Invalid mode: %s", config.Mode)
		os.Exit(1)
	}

	totalTime := time.Since(startTime)
	summary.TimesMs["total"] = totalTime.Milliseconds()

	// Write metrics
	metricsPath := filepath.Join(runPath, "metrics", "run_summary.json")
	if err := writeMetrics(metricsPath, summary); err != nil {
		common.LogError("MASTER", "Failed to write metrics: %v", err)
		os.Exit(1)
	}

	common.LogInfo("MASTER", "Run completed: total_time=%dms", totalTime.Milliseconds())
}

// runBaseline executes a baseline (unmitigated) WordCount run
func runBaseline(config RunConfig, runPath string, summary *RunSummary) {
	common.LogInfo("MASTER", "Running baseline mode")

	// Run mappers
	mapStart := time.Now()
	if err := runMappers(config, runPath, ""); err != nil {
		common.LogError("MASTER", "Map phase failed: %v", err)
		os.Exit(1)
	}
	summary.TimesMs["map"] = time.Since(mapStart).Milliseconds()

	// Shuffle
	shuffleStart := time.Now()
	if err := shuffle(config, runPath); err != nil {
		common.LogError("MASTER", "Shuffle failed: %v", err)
		os.Exit(1)
	}
	summary.TimesMs["shuffle"] = time.Since(shuffleStart).Milliseconds()

	// Run reducers
	reduceStart := time.Now()
	if err := runReducers(config, runPath); err != nil {
		common.LogError("MASTER", "Reduce phase failed: %v", err)
		os.Exit(1)
	}
	summary.TimesMs["reduce"] = time.Since(reduceStart).Milliseconds()

	// Merge reducer outputs
	mergeStart := time.Now()
	if err := mergeReducerOutputs(config, runPath); err != nil {
		common.LogError("MASTER", "Merge failed: %v", err)
		os.Exit(1)
	}
	summary.TimesMs["merge"] = time.Since(mergeStart).Milliseconds()

	// Collect metrics
	collectMetrics(config, runPath, summary)

	common.LogInfo("MASTER", "Baseline mode completed")
}

// runMitigation executes a mitigation WordCount run with skew handling
func runMitigation(config RunConfig, runPath string, summary *RunSummary) {
	common.LogInfo("MASTER", "Running mitigation mode")

	// Phase 1: Sample
	sampleStart := time.Now()
	if err := runMappersSample(config, runPath); err != nil {
		common.LogError("MASTER", "Sample phase failed: %v", err)
		os.Exit(1)
	}
	summary.TimesMs["sample"] = time.Since(sampleStart).Milliseconds()

	// Phase 2: Analyze and create partition plan
	planStart := time.Now()
	planPath, heavyHitters, err := createPartitionPlan(config, runPath)
	if err != nil {
		common.LogError("MASTER", "Failed to create partition plan: %v", err)
		os.Exit(1)
	}
	summary.TimesMs["plan"] = time.Since(planStart).Milliseconds()
	summary.HeavyHitters = heavyHitters

	// Phase 3: Execute with plan
	executeStart := time.Now()
	if err := runMappers(config, runPath, planPath); err != nil {
		common.LogError("MASTER", "Execute phase failed: %v", err)
		os.Exit(1)
	}
	summary.TimesMs["map"] = time.Since(executeStart).Milliseconds()

	// Shuffle
	shuffleStart := time.Now()
	if err := shuffle(config, runPath); err != nil {
		common.LogError("MASTER", "Shuffle failed: %v", err)
		os.Exit(1)
	}
	summary.TimesMs["shuffle"] = time.Since(shuffleStart).Milliseconds()

	// Run reducers
	reduceStart := time.Now()
	if err := runReducers(config, runPath); err != nil {
		common.LogError("MASTER", "Reduce phase failed: %v", err)
		os.Exit(1)
	}
	summary.TimesMs["reduce"] = time.Since(reduceStart).Milliseconds()

	// Merge and unsalt
	mergeStart := time.Now()
	if err := runMergeUnsalt(config, runPath); err != nil {
		common.LogError("MASTER", "Merge unsalt failed: %v", err)
		os.Exit(1)
	}
	summary.TimesMs["merge"] = time.Since(mergeStart).Milliseconds()

	// Collect metrics
	collectMetrics(config, runPath, summary)

	common.LogInfo("MASTER", "Mitigation mode completed")
}

// runMappersSample runs mappers in sample mode for skew detection
func runMappersSample(config RunConfig, runPath string) error {
	common.LogInfo("MASTER", "Running mappers in sample mode")

	semaphore := make(chan struct{}, config.MaxParallelMaps)
	var wg sync.WaitGroup
	var mu sync.Mutex
	errors := make([]error, 0)

	for i := 0; i < config.M; i++ {
		wg.Add(1)
		go func(mapID int) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			shardPath := filepath.Join(runPath, "shards", fmt.Sprintf("shard_%03d.jsonl", mapID))
			outDir := filepath.Join(runPath, "intermediate", fmt.Sprintf("map_%d", mapID))

			cmd := buildMapperCommand("sample", mapID, shardPath, outDir, config.SampleRate, 0, "", config.Seed, "wordcount")
			if err := runCommand(cmd, filepath.Join(runPath, "logs", fmt.Sprintf("mapper_%d_sample.log", mapID))); err != nil {
				mu.Lock()
				errors = append(errors, fmt.Errorf("mapper %d failed: %w", mapID, err))
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	if len(errors) > 0 {
		return fmt.Errorf("mapper errors: %v", errors)
	}

	return nil
}

// runMappers runs mappers in execute mode
func runMappers(config RunConfig, runPath string, planPath string) error {
	common.LogInfo("MASTER", "Running mappers in execute mode")

	semaphore := make(chan struct{}, config.MaxParallelMaps)
	var wg sync.WaitGroup
	var mu sync.Mutex
	errors := make([]error, 0)

	for i := 0; i < config.M; i++ {
		wg.Add(1)
		go func(mapID int) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			shardPath := filepath.Join(runPath, "shards", fmt.Sprintf("shard_%03d.jsonl", mapID))
			outDir := filepath.Join(runPath, "intermediate", fmt.Sprintf("map_%d", mapID))

			cmd := buildMapperCommand("execute", mapID, shardPath, outDir, 0, config.R, planPath, config.Seed, "wordcount")
			if err := runCommand(cmd, filepath.Join(runPath, "logs", fmt.Sprintf("mapper_%d_execute.log", mapID))); err != nil {
				mu.Lock()
				errors = append(errors, fmt.Errorf("mapper %d failed: %w", mapID, err))
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	if len(errors) > 0 {
		return fmt.Errorf("mapper errors: %v", errors)
	}

	return nil
}

// mergeReducerOutputs concatenates reducer outputs for baseline mode
func mergeReducerOutputs(config RunConfig, runPath string) error {
	common.LogInfo("MASTER", "Merging reducer outputs (baseline)")

	finalPath := filepath.Join(runPath, "output", "final.jsonl")
	outFile, err := os.Create(finalPath)
	if err != nil {
		return fmt.Errorf("failed to create final output: %w", err)
	}
	defer outFile.Close()

	encoder := json.NewEncoder(outFile)

	for r := 0; r < config.R; r++ {
		reducePath := filepath.Join(runPath, "output", fmt.Sprintf("reduce_%d.jsonl", r))
		if err := common.StreamJSONL(reducePath, func(record common.OutputRecord) error {
			return encoder.Encode(record)
		}); err != nil {
			return fmt.Errorf("failed to stream reducer %d output: %w", r, err)
		}
	}

	return nil
}

// runMergeUnsalt runs merge_unsalt to combine and desalt reducer outputs
func runMergeUnsalt(config RunConfig, runPath string) error {
	common.LogInfo("MASTER", "Running merge_unsalt")

	inputsGlob := filepath.Join(runPath, "output", "reduce_*.jsonl")
	outPath := filepath.Join(runPath, "output", "final.jsonl")

	cmd := buildMergeUnsaltCommand(inputsGlob, outPath)
	return runCommand(cmd, filepath.Join(runPath, "logs", "merge_unsalt.log"))
}

// buildMergeUnsaltCommand creates a command to run merge_unsalt
func buildMergeUnsaltCommand(inputsGlob, outPath string) *exec.Cmd {
	if _, err := os.Stat("bin/merge_unsalt"); err == nil {
		return exec.Command("bin/merge_unsalt",
			"--inputs-glob", inputsGlob,
			"--out", outPath,
		)
	}
	return exec.Command("go", "run", "./cmd/merge_unsalt",
		"--inputs-glob", inputsGlob,
		"--out", outPath,
	)
}

// createPartitionPlan analyzes sample data and creates a partition plan
func createPartitionPlan(config RunConfig, runPath string) (string, map[string]interface{}, error) {
	common.LogInfo("MASTER", "Creating partition plan")

	// Aggregate sample counts from all mappers
	aggregateCounts := make(map[string]int)
	for m := 0; m < config.M; m++ {
		samplePath := filepath.Join(runPath, "intermediate", fmt.Sprintf("map_%d", m), "sample_counts.json")
		if file, err := os.Open(samplePath); err == nil {
			var counts map[string]int
			if json.NewDecoder(file).Decode(&counts) == nil {
				for key, count := range counts {
					aggregateCounts[key] += count
				}
			}
			file.Close()
		}
	}

	// Identify heavy hitters
	totalSamples := 0
	for _, count := range aggregateCounts {
		totalSamples += count
	}

	threshold := int(float64(totalSamples) * config.HeavyTopPct)
	heavyHitters := make(map[string]interface{})
	plan := &common.PartitionPlan{
		R:     config.R,
		Heavy: make(map[string]common.HeavyKeyInfo),
	}

	for key, count := range aggregateCounts {
		if count >= threshold {
			plan.Heavy[key] = common.HeavyKeyInfo{
				Splits: config.FixedSplits,
			}
			heavyHitters[key] = map[string]interface{}{
				"sample_count":   count,
				"estimated_freq": float64(count) / float64(totalSamples),
			}
		}
	}

	// Write plan
	planPath := filepath.Join(runPath, "partition_plan.json")
	planFile, err := os.Create(planPath)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create plan file: %w", err)
	}
	defer planFile.Close()

	encoder := json.NewEncoder(planFile)
	if err := encoder.Encode(plan); err != nil {
		return "", nil, fmt.Errorf("failed to encode plan: %w", err)
	}

	return planPath, heavyHitters, nil
}

// collectMetrics reads reducer stats and computes load balancing metrics
func collectMetrics(config RunConfig, runPath string, summary *RunSummary) {
	summary.ReducerLoad.Bytes = make([]int64, config.R)
	summary.ReducerLoad.Records = make([]int64, config.R)

	for r := 0; r < config.R; r++ {
		statsPath := filepath.Join(runPath, "output", fmt.Sprintf("reduce_%d.stats.json", r))
		file, err := os.Open(statsPath)
		if err != nil {
			common.LogWarn("MASTER", "Failed to open reducer stats %d: %v", r, err)
			continue
		}

		var stats map[string]interface{}
		decoder := json.NewDecoder(file)
		if err := decoder.Decode(&stats); err != nil {
			file.Close()
			common.LogWarn("MASTER", "Failed to decode reducer stats %d: %v", r, err)
			continue
		}
		file.Close()

		if bytes, ok := stats["bytes_read"].(float64); ok {
			summary.ReducerLoad.Bytes[r] = int64(bytes)
		}
		if records, ok := stats["records_read"].(float64); ok {
			summary.ReducerLoad.Records[r] = int64(records)
		}
	}

	// Compute metrics
	covBytes, covRecords, maxMedBytes, maxMedRecords := common.ComputeReducerLoadMetrics(summary.ReducerLoad)
	summary.CoV = map[string]float64{
		"bytes":   covBytes,
		"records": covRecords,
	}
	summary.MaxMedianRatio = map[string]float64{
		"bytes":   maxMedBytes,
		"records": maxMedRecords,
	}
}
