package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"key_skew/internal/common"
	"key_skew/internal/jobs"
)

// PageRankConfig holds configuration for PageRank execution
type PageRankConfig struct {
	InputPath       string
	RunDir          string
	M               int
	R               int
	MaxParallelMaps int
	Iterations      int
	Damping         float64
	NumNodes        int
}

// PageRankSummary holds metrics and summary information for a PageRank run
type PageRankSummary struct {
	M                int                         `json:"M"`
	R                int                         `json:"R"`
	Iterations       int                         `json:"iterations"`
	Damping          float64                     `json:"damping"`
	NumNodes         int                         `json:"num_nodes"`
	TotalTimeMs      int64                       `json:"total_time_ms"`
	IterationTimes   map[string]int64            `json:"iteration_times_ms"` // "iter_001": time_ms
	IterationMetrics map[string]IterationMetrics `json:"iteration_metrics"`  // "iter_001": metrics
}

// IterationMetrics holds metrics for a single PageRank iteration
type IterationMetrics struct {
	MapTimeMs      int64                     `json:"map_time_ms"`
	ShuffleTimeMs  int64                     `json:"shuffle_time_ms"`
	ReduceTimeMs   int64                     `json:"reduce_time_ms"`
	MergeTimeMs    int64                     `json:"merge_time_ms"`
	TotalTimeMs    int64                     `json:"total_time_ms"`
	ReducerLoad    common.ReducerLoadMetrics `json:"reducer_load"`
	CoV            map[string]float64        `json:"cov"`
	MaxMedianRatio map[string]float64        `json:"max_median_ratio"`
}

// RunPageRank is the main entry point for PageRank execution
// It orchestrates multiple MapReduce iterations to compute PageRank
func RunPageRank() {
	runFlags := flag.NewFlagSet("run-pagerank", flag.ExitOnError)
	var config PageRankConfig
	runFlags.StringVar(&config.InputPath, "input", "", "Input graph state JSONL file path")
	runFlags.StringVar(&config.RunDir, "run-dir", "runs", "Base directory for runs")
	runFlags.IntVar(&config.M, "M", 8, "Number of mappers")
	runFlags.IntVar(&config.R, "R", 8, "Number of reducers")
	runFlags.IntVar(&config.MaxParallelMaps, "max-parallel-maps", 8, "Maximum parallel mappers")
	runFlags.IntVar(&config.Iterations, "iterations", 10, "Number of PageRank iterations")
	runFlags.Float64Var(&config.Damping, "damping", 0.85, "PageRank damping factor")
	runFlags.Parse(os.Args[2:]) // Parse flags after "run-pagerank"

	if config.InputPath == "" {
		fmt.Fprintf(os.Stderr, "Error: --input is required\n")
		os.Exit(1)
	}

	// Read metadata to get numNodes if available
	metaPath := config.InputPath + ".meta.json"
	if metaFile, err := os.Open(metaPath); err == nil {
		var meta map[string]interface{}
		if json.NewDecoder(metaFile).Decode(&meta) == nil {
			if numNodes, ok := meta["num_nodes"].(float64); ok {
				config.NumNodes = int(numNodes)
			}
		}
		metaFile.Close()
	}

	// If numNodes not in metadata, count from input file
	if config.NumNodes == 0 {
		file, err := os.Open(config.InputPath)
		if err == nil {
			scanner := bufio.NewScanner(file)
			count := 0
			for scanner.Scan() {
				if len(scanner.Bytes()) > 0 {
					count++
				}
			}
			file.Close()
			config.NumNodes = count
		}
	}

	if config.NumNodes == 0 {
		fmt.Fprintf(os.Stderr, "Error: could not determine number of nodes\n")
		os.Exit(1)
	}

	// Initialize logging
	if err := common.InitLogging(".", "master"); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logging: %v\n", err)
		os.Exit(1)
	}
	defer common.CloseLogging()

	common.LogInfo("MASTER", "Starting PageRank: iterations=%d, M=%d, R=%d, nodes=%d", config.Iterations, config.M, config.R, config.NumNodes)

	// Create run directory
	timestamp := time.Now().Format("20060102_150405")
	runPath := filepath.Join(config.RunDir, fmt.Sprintf("run_pagerank_%s", timestamp))
	if err := createPageRankDirectories(runPath, config.Iterations); err != nil {
		common.LogError("MASTER", "Failed to create run directories: %v", err)
		os.Exit(1)
	}

	// Update log directory
	common.CloseLogging()
	if err := common.InitLogging(filepath.Join(runPath, "logs"), "master"); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logging: %v\n", err)
		os.Exit(1)
	}

	startTime := time.Now()

	// Initialize summary
	var summary PageRankSummary
	summary.M = config.M
	summary.R = config.R
	summary.Iterations = config.Iterations
	summary.Damping = config.Damping
	summary.NumNodes = config.NumNodes
	summary.IterationTimes = make(map[string]int64)
	summary.IterationMetrics = make(map[string]IterationMetrics)

	// Copy original input graph file to run directory if it exists
	// The input path is the initialized graph state (JSONL), try to find the original edge list
	originalGraphPath := findOriginalGraphFile(config.InputPath)
	if originalGraphPath != "" {
		runGraphPath := filepath.Join(runPath, "input_graph.txt")
		if err := copyFile(originalGraphPath, runGraphPath); err != nil {
			common.LogWarn("MASTER", "Failed to copy original graph file: %v", err)
		} else {
			common.LogInfo("MASTER", "Copied input graph to run directory: %s", runGraphPath)
		}

		// Also copy metadata file if it exists
		metaPath := originalGraphPath + ".meta.json"
		if _, err := os.Stat(metaPath); err == nil {
			runMetaPath := filepath.Join(runPath, "input_graph.txt.meta.json")
			if err := copyFile(metaPath, runMetaPath); err == nil {
				common.LogInfo("MASTER", "Copied graph metadata to run directory")
			}
		}
	} else {
		// If we can't find the original, reconstruct it from the graph state
		common.LogInfo("MASTER", "Reconstructing edge list from graph state")
		if err := reconstructEdgeListFromGraphState(config.InputPath, filepath.Join(runPath, "input_graph.txt")); err != nil {
			common.LogWarn("MASTER", "Failed to reconstruct edge list: %v", err)
		} else {
			common.LogInfo("MASTER", "Reconstructed edge list and saved to run directory")
		}
	}

	// Copy initial input to iter_000
	iter0Path := filepath.Join(runPath, "iter_000", "input.jsonl")
	if err := copyFile(config.InputPath, iter0Path); err != nil {
		common.LogError("MASTER", "Failed to copy initial input: %v", err)
		os.Exit(1)
	}

	currentInput := iter0Path

	// Run iterations
	for iter := 1; iter <= config.Iterations; iter++ {
		iterKey := fmt.Sprintf("iter_%03d", iter)
		iterStart := time.Now()
		common.LogInfo("MASTER", "Starting iteration %d/%d", iter, config.Iterations)

		iterDir := filepath.Join(runPath, iterKey)
		if err := os.MkdirAll(iterDir, 0755); err != nil {
			common.LogError("MASTER", "Failed to create iteration directory: %v", err)
			os.Exit(1)
		}

		var iterMetrics IterationMetrics

		// Shard input (PageRank node records need to be wrapped in InputRecord format)
		if err := shardPageRankInput(currentInput, iterDir, config.M); err != nil {
			common.LogError("MASTER", "Iteration %d: failed to shard input: %v", iter, err)
			os.Exit(1)
		}

		// Run mappers
		mapStart := time.Now()
		if err := runPageRankMappers(config, iterDir); err != nil {
			common.LogError("MASTER", "Iteration %d: map phase failed: %v", iter, err)
			os.Exit(1)
		}
		iterMetrics.MapTimeMs = time.Since(mapStart).Milliseconds()

		// Shuffle
		shuffleStart := time.Now()
		shuffleConfig := RunConfig{M: config.M, R: config.R}
		if err := shuffle(shuffleConfig, iterDir); err != nil {
			common.LogError("MASTER", "Iteration %d: shuffle failed: %v", iter, err)
			os.Exit(1)
		}
		iterMetrics.ShuffleTimeMs = time.Since(shuffleStart).Milliseconds()

		// Run reducers
		reduceStart := time.Now()
		if err := runPageRankReducers(config, iterDir); err != nil {
			common.LogError("MASTER", "Iteration %d: reduce phase failed: %v", iter, err)
			os.Exit(1)
		}
		iterMetrics.ReduceTimeMs = time.Since(reduceStart).Milliseconds()

		// Merge reducer outputs into next iteration input
		mergeStart := time.Now()
		nextInput := filepath.Join(iterDir, "next_input.jsonl")
		if err := mergePageRankOutputs(config, iterDir, nextInput); err != nil {
			common.LogError("MASTER", "Iteration %d: merge failed: %v", iter, err)
			os.Exit(1)
		}
		iterMetrics.MergeTimeMs = time.Since(mergeStart).Milliseconds()

		// Collect reducer metrics for this iteration
		collectPageRankIterationMetrics(config, iterDir, &iterMetrics)

		iterElapsed := time.Since(iterStart)
		iterMetrics.TotalTimeMs = iterElapsed.Milliseconds()
		summary.IterationTimes[iterKey] = iterElapsed.Milliseconds()
		summary.IterationMetrics[iterKey] = iterMetrics

		currentInput = nextInput
		common.LogInfo("MASTER", "Iteration %d completed: elapsed=%dms", iter, iterElapsed.Milliseconds())
	}

	// Write final output
	finalDir := filepath.Join(runPath, "final")
	if err := os.MkdirAll(finalDir, 0755); err != nil {
		common.LogError("MASTER", "Failed to create final directory: %v", err)
		os.Exit(1)
	}

	// Write final ranks
	finalRanksPath := filepath.Join(finalDir, "ranks.jsonl")
	if err := copyFile(currentInput, finalRanksPath); err != nil {
		common.LogError("MASTER", "Failed to write final ranks: %v", err)
		os.Exit(1)
	}

	// Write sorted ranks
	if err := writeSortedRanks(currentInput, filepath.Join(finalDir, "ranks_sorted.jsonl")); err != nil {
		common.LogWarn("MASTER", "Failed to write sorted ranks: %v", err)
	}

	totalTime := time.Since(startTime)
	summary.TotalTimeMs = totalTime.Milliseconds()

	// Write metrics
	metricsPath := filepath.Join(runPath, "metrics", "run_summary.json")
	if err := writePageRankMetrics(metricsPath, summary); err != nil {
		common.LogError("MASTER", "Failed to write metrics: %v", err)
		os.Exit(1)
	}

	common.LogInfo("MASTER", "PageRank completed: iterations=%d, total_time=%dms", config.Iterations, totalTime.Milliseconds())
}

// createPageRankDirectories creates directory structure for PageRank run
func createPageRankDirectories(runPath string, iterations int) error {
	dirs := []string{
		filepath.Join(runPath, "logs"),
		filepath.Join(runPath, "iter_000"),
		filepath.Join(runPath, "final"),
		filepath.Join(runPath, "metrics"),
	}

	for i := 1; i <= iterations; i++ {
		iterDir := filepath.Join(runPath, fmt.Sprintf("iter_%03d", i))
		dirs = append(dirs, iterDir)
		dirs = append(dirs, filepath.Join(iterDir, "shards"))
		dirs = append(dirs, filepath.Join(iterDir, "intermediate"))
		dirs = append(dirs, filepath.Join(iterDir, "shuffle"))
		dirs = append(dirs, filepath.Join(iterDir, "output"))
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create %s: %w", dir, err)
		}
	}

	return nil
}

// shardPageRankInput shards PageRank node records into M shards
// Each node record is wrapped in InputRecord format for the mapper
func shardPageRankInput(inputPath string, iterDir string, M int) error {
	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	// Create shard files
	shardFiles := make([]*os.File, M)
	shardWriters := make([]*bufio.Writer, M)
	for i := 0; i < M; i++ {
		shardPath := filepath.Join(iterDir, "shards", fmt.Sprintf("shard_%03d.jsonl", i))
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

	// Distribute lines round-robin, wrapping each in InputRecord format
	scanner := bufio.NewScanner(file)
	shardIdx := 0
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 {
			// Wrap the node record JSON in InputRecord format
			inputRecord := common.InputRecord{Text: line}
			jsonBytes, err := json.Marshal(inputRecord)
			if err != nil {
				return fmt.Errorf("failed to marshal input record: %w", err)
			}
			if _, err := shardWriters[shardIdx].Write(jsonBytes); err != nil {
				return fmt.Errorf("failed to write to shard: %w", err)
			}
			if _, err := shardWriters[shardIdx].WriteString("\n"); err != nil {
				return fmt.Errorf("failed to write newline: %w", err)
			}
			shardIdx = (shardIdx + 1) % M
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input: %w", err)
	}

	return nil
}

// runPageRankMappers runs mappers for a PageRank iteration
func runPageRankMappers(config PageRankConfig, iterDir string) error {
	common.LogInfo("MASTER", "Running PageRank mappers")

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

			shardPath := filepath.Join(iterDir, "shards", fmt.Sprintf("shard_%03d.jsonl", mapID))
			outDir := filepath.Join(iterDir, "intermediate", fmt.Sprintf("map_%d", mapID))

			cmd := buildMapperCommand("execute", mapID, shardPath, outDir, 0, config.R, "", 0, "pagerank")
			if err := runCommand(cmd, filepath.Join(iterDir, "..", "..", "logs", fmt.Sprintf("mapper_%d_execute.log", mapID))); err != nil {
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

// runPageRankReducers runs reducers for a PageRank iteration
func runPageRankReducers(config PageRankConfig, iterDir string) error {
	common.LogInfo("MASTER", "Running PageRank reducers")

	var wg sync.WaitGroup
	var mu sync.Mutex
	errors := make([]error, 0)

	for r := 0; r < config.R; r++ {
		wg.Add(1)
		go func(reduceID int) {
			defer wg.Done()

			inputsPath := filepath.Join(iterDir, "shuffle", fmt.Sprintf("reduce_%d_inputs.txt", reduceID))
			outPath := filepath.Join(iterDir, "output", fmt.Sprintf("reduce_%d.jsonl", reduceID))

			cmd := buildReducerCommand(reduceID, inputsPath, outPath, "pagerank", config.Damping, config.NumNodes)
			if err := runCommand(cmd, filepath.Join(iterDir, "..", "..", "logs", fmt.Sprintf("reducer_%d.log", reduceID))); err != nil {
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

// mergePageRankOutputs concatenates reducer outputs into next iteration input
func mergePageRankOutputs(config PageRankConfig, iterDir string, outPath string) error {
	common.LogInfo("MASTER", "Merging PageRank reducer outputs")

	outFile, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	writer := bufio.NewWriter(outFile)
	defer writer.Flush()

	for r := 0; r < config.R; r++ {
		reducePath := filepath.Join(iterDir, "output", fmt.Sprintf("reduce_%d.jsonl", r))
		file, err := os.Open(reducePath)
		if err != nil {
			continue // Skip if file doesn't exist
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) > 0 {
				if _, err := writer.Write(line); err != nil {
					file.Close()
					return fmt.Errorf("failed to write line: %w", err)
				}
				if _, err := writer.WriteString("\n"); err != nil {
					file.Close()
					return fmt.Errorf("failed to write newline: %w", err)
				}
			}
		}
		file.Close()

		if err := scanner.Err(); err != nil {
			return fmt.Errorf("error reading reducer output: %w", err)
		}
	}

	return nil
}

// writeSortedRanks writes final ranks sorted by descending rank
func writeSortedRanks(inputPath string, outPath string) error {
	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input: %w", err)
	}
	defer file.Close()

	var records []jobs.PageRankNodeRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var record jobs.PageRankNodeRecord
		if err := json.Unmarshal(line, &record); err != nil {
			continue // Skip malformed records
		}
		records = append(records, record)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input: %w", err)
	}

	// Sort by descending rank
	sort.Slice(records, func(i, j int) bool {
		return records[i].Rank > records[j].Rank
	})

	// Write sorted output
	outFile, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("failed to create output: %w", err)
	}
	defer outFile.Close()

	encoder := json.NewEncoder(outFile)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			return fmt.Errorf("failed to encode record: %w", err)
		}
	}

	return nil
}

// collectPageRankIterationMetrics reads reducer stats and computes load balancing metrics for an iteration
func collectPageRankIterationMetrics(config PageRankConfig, iterDir string, metrics *IterationMetrics) {
	metrics.ReducerLoad.Bytes = make([]int64, config.R)
	metrics.ReducerLoad.Records = make([]int64, config.R)

	for r := 0; r < config.R; r++ {
		statsPath := filepath.Join(iterDir, "output", fmt.Sprintf("reduce_%d.stats.json", r))
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
			metrics.ReducerLoad.Bytes[r] = int64(bytes)
		}
		if records, ok := stats["records_read"].(float64); ok {
			metrics.ReducerLoad.Records[r] = int64(records)
		}
	}

	// Compute metrics
	covBytes, covRecords, maxMedBytes, maxMedRecords := common.ComputeReducerLoadMetrics(metrics.ReducerLoad)
	metrics.CoV = map[string]float64{
		"bytes":   covBytes,
		"records": covRecords,
	}
	metrics.MaxMedianRatio = map[string]float64{
		"bytes":   maxMedBytes,
		"records": maxMedRecords,
	}
}

// writePageRankMetrics writes PageRank run summary metrics to a JSON file
func writePageRankMetrics(metricsPath string, summary PageRankSummary) error {
	// Create metrics directory if needed
	if err := os.MkdirAll(filepath.Dir(metricsPath), 0755); err != nil {
		return fmt.Errorf("failed to create metrics directory: %w", err)
	}

	file, err := os.Create(metricsPath)
	if err != nil {
		return fmt.Errorf("failed to create metrics file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(summary)
}

func findOriginalGraphFile(graphStatePath string) string {

	baseDir := filepath.Dir(graphStatePath)
	baseName := filepath.Base(graphStatePath)

	// Remove common suffixes
	candidates := []string{
		filepath.Join(baseDir, strings.TrimSuffix(baseName, "_init.jsonl")+".txt"),
		filepath.Join(baseDir, strings.TrimSuffix(baseName, "_init.jsonl")),
		filepath.Join(baseDir, strings.TrimSuffix(baseName, ".jsonl")+".txt"),
		filepath.Join(baseDir, strings.TrimSuffix(baseName, ".jsonl")),
		// Try common graph file names
		filepath.Join(baseDir, "graph.txt"),
		filepath.Join(baseDir, "graph_zipf.txt"),
		filepath.Join(baseDir, "graph_skewed.txt"),
		filepath.Join(baseDir, "test_graph.txt"),
	}

	// Also try parent directory
	parentDir := filepath.Dir(baseDir)
	candidates = append(candidates,
		filepath.Join(parentDir, "graph.txt"),
		filepath.Join(parentDir, "graph_zipf.txt"),
		filepath.Join(parentDir, "graph_skewed.txt"),
		filepath.Join(parentDir, "test_graph.txt"),
	)

	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}

	return ""
}

func reconstructEdgeListFromGraphState(graphStatePath, outputPath string) error {
	file, err := os.Open(graphStatePath)
	if err != nil {
		return fmt.Errorf("failed to open graph state: %w", err)
	}
	defer file.Close()

	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	writer := bufio.NewWriter(outFile)
	defer writer.Flush()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var nodeRecord jobs.PageRankNodeRecord
		if err := json.Unmarshal(line, &nodeRecord); err != nil {
			continue // Skip malformed records
		}

		// Write edges: node -> neighbor
		for _, neighbor := range nodeRecord.Neighbors {
			if _, err := writer.WriteString(fmt.Sprintf("%s %s\n", nodeRecord.Node, neighbor)); err != nil {
				return fmt.Errorf("failed to write edge: %w", err)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading graph state: %w", err)
	}

	return nil
}
