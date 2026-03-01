package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"key_skew/internal/common"
)

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

func main() {
	if len(os.Args) < 2 || os.Args[1] != "run" {
		fmt.Fprintf(os.Stderr, "Usage: %s run [flags]\n", os.Args[0])
		os.Exit(1)
	}

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
	runPath := filepath.Join(config.RunDir, fmt.Sprintf("run_%s", timestamp))
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

func createRunDirectories(runPath string) error {
	dirs := []string{
		filepath.Join(runPath, "shards"),
		filepath.Join(runPath, "intermediate"),
		filepath.Join(runPath, "shuffle"),
		filepath.Join(runPath, "output"),
		filepath.Join(runPath, "metrics"),
		filepath.Join(runPath, "logs"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create %s: %w", dir, err)
		}
	}

	return nil
}

func shardInput(inputPath string, runPath string, M int) error {
	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	// Create shard files
	shardFiles := make([]*os.File, M)
	shardWriters := make([]*bufio.Writer, M)
	for i := 0; i < M; i++ {
		shardPath := filepath.Join(runPath, "shards", fmt.Sprintf("shard_%03d.jsonl", i))
		shardFile, err := os.Create(shardPath)
		if err != nil {
			return fmt.Errorf("failed to create shard file: %w", err)
		}
		shardFiles[i] = shardFile
		shardWriters[i] = bufio.NewWriter(shardFile)
	}
	defer func() {
		for i, w := range shardWriters {
			w.Flush()
			shardFiles[i].Close()
		}
	}()

	// Distribute lines round-robin
	scanner := bufio.NewScanner(file)
	shardIdx := 0
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 {
			if _, err := shardWriters[shardIdx].WriteString(line + "\n"); err != nil {
				return fmt.Errorf("failed to write to shard: %w", err)
			}
			shardIdx = (shardIdx + 1) % M
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input: %w", err)
	}

	return nil
}

func runBaseline(config RunConfig, runPath string, summary *RunSummary) {
	// Map phase
	mapStart := time.Now()
	if err := runMappers(config, runPath, ""); err != nil {
		common.LogError("MASTER", "Map phase failed: %v", err)
		os.Exit(1)
	}
	summary.TimesMs["map_pass2"] = time.Since(mapStart).Milliseconds()

	// Shuffle
	if err := shuffle(config, runPath); err != nil {
		common.LogError("MASTER", "Shuffle failed: %v", err)
		os.Exit(1)
	}

	// Reduce phase
	reduceStart := time.Now()
	if err := runReducers(config, runPath); err != nil {
		common.LogError("MASTER", "Reduce phase failed: %v", err)
		os.Exit(1)
	}
	summary.TimesMs["reduce"] = time.Since(reduceStart).Milliseconds()

	// Merge reducer outputs (simple concatenation for baseline)
	if err := mergeReducerOutputs(config, runPath); err != nil {
		common.LogError("MASTER", "Merge failed: %v", err)
		os.Exit(1)
	}

	// Collect metrics
	collectMetrics(config, runPath, summary)
}

func runMitigation(config RunConfig, runPath string, summary *RunSummary) {
	// Map Pass 1: Sampling
	map1Start := time.Now()
	if err := runMappersSample(config, runPath); err != nil {
		common.LogError("MASTER", "Sampling phase failed: %v", err)
		os.Exit(1)
	}
	summary.TimesMs["map_pass1"] = time.Since(map1Start).Milliseconds()

	// Aggregate samples and create plan
	planPath, heavyHitters, err := createPartitionPlan(config, runPath)
	if err != nil {
		common.LogError("MASTER", "Failed to create partition plan: %v", err)
		os.Exit(1)
	}
	summary.HeavyHitters = heavyHitters

	// Map Pass 2: Execution
	map2Start := time.Now()
	if err := runMappers(config, runPath, planPath); err != nil {
		common.LogError("MASTER", "Execution phase failed: %v", err)
		os.Exit(1)
	}
	summary.TimesMs["map_pass2"] = time.Since(map2Start).Milliseconds()

	// Shuffle
	if err := shuffle(config, runPath); err != nil {
		common.LogError("MASTER", "Shuffle failed: %v", err)
		os.Exit(1)
	}

	// Reduce phase
	reduceStart := time.Now()
	if err := runReducers(config, runPath); err != nil {
		common.LogError("MASTER", "Reduce phase failed: %v", err)
		os.Exit(1)
	}
	summary.TimesMs["reduce"] = time.Since(reduceStart).Milliseconds()

	// Merge unsalt
	mergeStart := time.Now()
	if err := runMergeUnsalt(config, runPath); err != nil {
		common.LogError("MASTER", "Merge unsalt failed: %v", err)
		os.Exit(1)
	}
	summary.TimesMs["merge"] = time.Since(mergeStart).Milliseconds()

	// Collect metrics
	collectMetrics(config, runPath, summary)
}

func runMappersSample(config RunConfig, runPath string) error {
	common.LogInfo("MASTER", "Running mappers in sample mode")

	// Worker pool
	semaphore := make(chan struct{}, config.MaxParallelMaps)
	var wg sync.WaitGroup
	var mu sync.Mutex
	errors := make([]error, 0)

	for i := 0; i < config.M; i++ {
		wg.Add(1)
		go func(mapID int) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release

			shardPath := filepath.Join(runPath, "shards", fmt.Sprintf("shard_%03d.jsonl", mapID))
			outDir := filepath.Join(runPath, "intermediate", fmt.Sprintf("map_%d", mapID))

			cmd := buildMapperCommand("sample", mapID, shardPath, outDir, config.SampleRate, 0, "", config.Seed)
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

func runMappers(config RunConfig, runPath string, planPath string) error {
	common.LogInfo("MASTER", "Running mappers in execute mode")

	// Worker pool
	semaphore := make(chan struct{}, config.MaxParallelMaps)
	var wg sync.WaitGroup
	var mu sync.Mutex
	errors := make([]error, 0)

	for i := 0; i < config.M; i++ {
		wg.Add(1)
		go func(mapID int) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release

			shardPath := filepath.Join(runPath, "shards", fmt.Sprintf("shard_%03d.jsonl", mapID))
			outDir := filepath.Join(runPath, "intermediate", fmt.Sprintf("map_%d", mapID))

			cmd := buildMapperCommand("execute", mapID, shardPath, outDir, 0, config.R, planPath, config.Seed)
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

func buildMapperCommand(mode string, mapID int, shardPath, outDir string, sampleRate float64, R int, planPath string, seed int64) *exec.Cmd {
	// Try binary first, fallback to go run
	var cmd *exec.Cmd
	if _, err := os.Stat("bin/mapper"); err == nil {
		cmd = exec.Command("bin/mapper",
			"--mode", mode,
			"--map-id", strconv.Itoa(mapID),
			"--shard", shardPath,
			"--outdir", outDir,
		)
	} else {
		cmd = exec.Command("go", "run", "./cmd/mapper",
			"--mode", mode,
			"--map-id", strconv.Itoa(mapID),
			"--shard", shardPath,
			"--outdir", outDir,
		)
	}

	if mode == "sample" {
		cmd.Args = append(cmd.Args, "--sample-rate", fmt.Sprintf("%.4f", sampleRate))
		cmd.Args = append(cmd.Args, "--seed", strconv.FormatInt(seed, 10))
	} else {
		cmd.Args = append(cmd.Args, "--R", strconv.Itoa(R))
		if planPath != "" {
			cmd.Args = append(cmd.Args, "--plan", planPath)
		}
	}

	return cmd
}

func shuffle(config RunConfig, runPath string) error {
	common.LogInfo("MASTER", "Shuffling partition files")

	// For each reducer, collect all partition files that belong to it
	for r := 0; r < config.R; r++ {
		var inputFiles []string
		for m := 0; m < config.M; m++ {
			partPath := filepath.Join(runPath, "intermediate", fmt.Sprintf("map_%d", m), fmt.Sprintf("part_%03d.jsonl", r))
			if _, err := os.Stat(partPath); err == nil {
				inputFiles = append(inputFiles, partPath)
			}
		}

		inputsPath := filepath.Join(runPath, "shuffle", fmt.Sprintf("reduce_%d_inputs.txt", r))
		if err := common.WriteReducerInputs(inputsPath, inputFiles); err != nil {
			return fmt.Errorf("failed to write reducer inputs: %w", err)
		}
	}

	return nil
}

func runReducers(config RunConfig, runPath string) error {
	common.LogInfo("MASTER", "Running reducers")

	var wg sync.WaitGroup
	var mu sync.Mutex
	errors := make([]error, 0)

	for r := 0; r < config.R; r++ {
		wg.Add(1)
		go func(reduceID int) {
			defer wg.Done()

			inputsPath := filepath.Join(runPath, "shuffle", fmt.Sprintf("reduce_%d_inputs.txt", reduceID))
			outPath := filepath.Join(runPath, "output", fmt.Sprintf("reduce_%d.jsonl", reduceID))

			cmd := buildReducerCommand(reduceID, inputsPath, outPath)
			if err := runCommand(cmd, filepath.Join(runPath, "logs", fmt.Sprintf("reducer_%d.log", reduceID))); err != nil {
				mu.Lock()
				errors = append(errors, fmt.Errorf("reducer %d failed: %w", reduceID, err))
				mu.Unlock()
			}
		}(r)
	}

	wg.Wait()

	if len(errors) > 0 {
		return fmt.Errorf("reducer errors: %v", errors)
	}

	return nil
}

func buildReducerCommand(reduceID int, inputsPath, outPath string) *exec.Cmd {
	if _, err := os.Stat("bin/reducer"); err == nil {
		return exec.Command("bin/reducer",
			"--reduce-id", strconv.Itoa(reduceID),
			"--inputs", inputsPath,
			"--out", outPath,
		)
	}
	return exec.Command("go", "run", "./cmd/reducer",
		"--reduce-id", strconv.Itoa(reduceID),
		"--inputs", inputsPath,
		"--out", outPath,
	)
}

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
		if err := common.StreamJSONL(reducePath, func(record common.KV) error {
			return encoder.Encode(record)
		}); err != nil {
			return fmt.Errorf("failed to stream reducer %d output: %w", r, err)
		}
	}

	return nil
}

func runMergeUnsalt(config RunConfig, runPath string) error {
	common.LogInfo("MASTER", "Running merge_unsalt")

	inputsGlob := filepath.Join(runPath, "output", "reduce_*.jsonl")
	outPath := filepath.Join(runPath, "output", "final.jsonl")

	cmd := buildMergeUnsaltCommand(inputsGlob, outPath)
	return runCommand(cmd, filepath.Join(runPath, "logs", "merge_unsalt.log"))
}

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

func runCommand(cmd *exec.Cmd, logPath string) error {
	logFile, err := os.Create(logPath)
	if err != nil {
		return fmt.Errorf("failed to create log file: %w", err)
	}
	defer logFile.Close()

	cmd.Stdout = logFile
	cmd.Stderr = logFile

	_, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("command failed: %w", err)
	}

	return nil
}

func createPartitionPlan(config RunConfig, runPath string) (string, map[string]interface{}, error) {
	common.LogInfo("MASTER", "Creating partition plan from samples")

	// Aggregate sample counts from all mappers
	aggregatedCounts := make(map[string]int)
	for m := 0; m < config.M; m++ {
		samplePath := filepath.Join(runPath, "intermediate", fmt.Sprintf("map_%d", m), "sample_counts.json")
		file, err := os.Open(samplePath)
		if err != nil {
			return "", nil, fmt.Errorf("failed to open sample file: %w", err)
		}

		var counts map[string]int
		decoder := json.NewDecoder(file)
		if err := decoder.Decode(&counts); err != nil {
			file.Close()
			return "", nil, fmt.Errorf("failed to decode sample counts: %w", err)
		}
		file.Close()

		for key, count := range counts {
			aggregatedCounts[key] += count
		}
	}

	// Sort by count and identify heavy hitters
	type keyCount struct {
		key   string
		count int
	}
	keyCounts := make([]keyCount, 0, len(aggregatedCounts))
	for key, count := range aggregatedCounts {
		keyCounts = append(keyCounts, keyCount{key, count})
	}
	sort.Slice(keyCounts, func(i, j int) bool {
		return keyCounts[i].count > keyCounts[j].count
	})

	// Select top heavy_top_pct keys
	numHeavy := int(float64(len(keyCounts)) * config.HeavyTopPct)
	if numHeavy < 1 {
		numHeavy = 1
	}
	if numHeavy > len(keyCounts) {
		numHeavy = len(keyCounts)
	}

	plan := common.PartitionPlan{
		R:     config.R,
		Heavy: make(map[string]common.HeavyKeyInfo),
	}

	heavyHitters := make(map[string]interface{})
	heavyKeys := make([]string, 0)

	for i := 0; i < numHeavy; i++ {
		key := keyCounts[i].key
		splits := config.FixedSplits
		if splits > config.R {
			splits = config.R
		}
		plan.Heavy[key] = common.HeavyKeyInfo{Splits: splits}
		heavyKeys = append(heavyKeys, key)
		heavyHitters[key] = map[string]interface{}{
			"count":  keyCounts[i].count,
			"splits": splits,
		}
	}

	// Truncate to top 50 for summary
	if len(heavyKeys) > 50 {
		heavyKeys = heavyKeys[:50]
	}
	heavyHitters["top_keys"] = heavyKeys
	heavyHitters["count"] = len(plan.Heavy)
	heavyHitters["total_splits"] = numHeavy * config.FixedSplits

	// Write plan
	planPath := filepath.Join(runPath, "partition_plan.json")
	file, err := os.Create(planPath)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create plan file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(plan); err != nil {
		return "", nil, fmt.Errorf("failed to encode plan: %w", err)
	}

	common.LogInfo("MASTER", "Created partition plan: %d heavy hitters", len(plan.Heavy))
	return planPath, heavyHitters, nil
}

func collectMetrics(config RunConfig, runPath string, summary *RunSummary) {
	// Read reducer stats
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

func writeMetrics(metricsPath string, summary RunSummary) error {
	file, err := os.Create(metricsPath)
	if err != nil {
		return fmt.Errorf("failed to create metrics file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(summary)
}
