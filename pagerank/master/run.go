package master

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"key_skew/common/common"
	commonmaster "key_skew/common/master"
	_ "key_skew/pagerank/jobs"
	pagerankjobs "key_skew/pagerank/jobs"
)

type PageRankConfig struct {
	InputPath       string
	RunDir          string
	M               int
	R               int
	MaxParallelMaps int
	Iterations      int
	Damping         float64
	NumNodes        int
	Mode            string
	SampleRate      float64
	HeavyTopPct     float64
	FixedSplits     int
	Seed            int64
}

type PageRankSummary struct {
	Mode             string                      `json:"mode"`
	M                int                         `json:"M"`
	R                int                         `json:"R"`
	Iterations       int                         `json:"iterations"`
	Damping          float64                     `json:"damping"`
	NumNodes         int                         `json:"num_nodes"`
	TotalTimeMs      int64                       `json:"total_time_ms"`
	IterationTimes   map[string]int64            `json:"iteration_times_ms"`
	IterationMetrics map[string]IterationMetrics `json:"iteration_metrics"`
}

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
	runFlags.StringVar(&config.Mode, "mode", "baseline", "Mode: baseline or mitigation")
	runFlags.Float64Var(&config.SampleRate, "sample-rate", 0.01, "Sampling rate (mitigation mode only)")
	runFlags.Float64Var(&config.HeavyTopPct, "heavy-top-pct", 0.01, "Top percentage for heavy hitters (mitigation mode only)")
	runFlags.IntVar(&config.FixedSplits, "fixed-splits", 8, "Fixed number of splits for heavy keys (mitigation mode only)")
	runFlags.Int64Var(&config.Seed, "seed", 0, "Global seed for sampling")
	runFlags.Parse(os.Args[2:])

	if config.InputPath == "" {
		fmt.Fprintf(os.Stderr, "Error: --input is required\n")
		os.Exit(1)
	}

	if config.Mode != "baseline" && config.Mode != "mitigation" {
		fmt.Fprintf(os.Stderr, "Error: --mode must be 'baseline' or 'mitigation'\n")
		os.Exit(1)
	}

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

	if err := common.InitLogging(".", "master"); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logging: %v\n", err)
		os.Exit(1)
	}
	defer common.CloseLogging()

	common.LogInfo("MASTER", "Starting PageRank: mode=%s, iterations=%d, M=%d, R=%d, nodes=%d", config.Mode, config.Iterations, config.M, config.R, config.NumNodes)

	timestamp := time.Now().Format("20060102_150405")
	runPath := filepath.Join(config.RunDir, fmt.Sprintf("run_pagerank_%s_%s", config.Mode, timestamp))
	if err := createPageRankDirectories(runPath, config.Iterations); err != nil {
		common.LogError("MASTER", "Failed to create run directories: %v", err)
		os.Exit(1)
	}

	common.CloseLogging()
	if err := common.InitLogging(filepath.Join(runPath, "logs"), "master"); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logging: %v\n", err)
		os.Exit(1)
	}

	startTime := time.Now()

	var summary PageRankSummary
	summary.Mode = config.Mode
	summary.M = config.M
	summary.R = config.R
	summary.Iterations = config.Iterations
	summary.Damping = config.Damping
	summary.NumNodes = config.NumNodes
	summary.IterationTimes = make(map[string]int64)
	summary.IterationMetrics = make(map[string]IterationMetrics)

	originalGraphPath := findOriginalGraphFile(config.InputPath)
	if originalGraphPath != "" {
		runGraphPath := filepath.Join(runPath, "input_graph.txt")
		if err := commonmaster.CopyFile(originalGraphPath, runGraphPath); err != nil {
			common.LogWarn("MASTER", "Failed to copy original graph file: %v", err)
		} else {
			common.LogInfo("MASTER", "Copied input graph to run directory: %s", runGraphPath)
		}

		metaPath := originalGraphPath + ".meta.json"
		if _, err := os.Stat(metaPath); err == nil {
			runMetaPath := filepath.Join(runPath, "input_graph.txt.meta.json")
			if err := commonmaster.CopyFile(metaPath, runMetaPath); err == nil {
				common.LogInfo("MASTER", "Copied graph metadata to run directory")
			}
		}
	} else {
		common.LogInfo("MASTER", "Reconstructing edge list from graph state")
		if err := reconstructEdgeListFromGraphState(config.InputPath, filepath.Join(runPath, "input_graph.txt")); err != nil {
			common.LogWarn("MASTER", "Failed to reconstruct edge list: %v", err)
		} else {
			common.LogInfo("MASTER", "Reconstructed edge list and saved to run directory")
		}
	}

	iter0Path := filepath.Join(runPath, "iter_000", "input.jsonl")
	if err := commonmaster.CopyFile(config.InputPath, iter0Path); err != nil {
		common.LogError("MASTER", "Failed to copy initial input: %v", err)
		os.Exit(1)
	}

	currentInput := iter0Path

	var planPath string
	if config.Mode == "mitigation" {
		iter0Dir := filepath.Join(runPath, "iter_000")
		if err := shardPageRankInput(iter0Path, iter0Dir, config.M); err != nil {
			common.LogError("MASTER", "Failed to shard input for sampling: %v", err)
			os.Exit(1)
		}
		common.LogInfo("MASTER", "Running sampling phase to detect heavy destination nodes")
		sampleStart := time.Now()
		if err := runPageRankMappersSample(config, runPath); err != nil {
			common.LogError("MASTER", "Sample phase failed: %v", err)
			os.Exit(1)
		}
		common.LogInfo("MASTER", "Sampling completed: elapsed=%dms", time.Since(sampleStart).Milliseconds())

		planStart := time.Now()
		var err error
		planPath, err = createPageRankPartitionPlan(config, runPath)
		if err != nil {
			common.LogError("MASTER", "Failed to create partition plan: %v", err)
			os.Exit(1)
		}
		common.LogInfo("MASTER", "Partition plan created: elapsed=%dms", time.Since(planStart).Milliseconds())
	}

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

		if err := shardPageRankInput(currentInput, iterDir, config.M); err != nil {
			common.LogError("MASTER", "Iteration %d: failed to shard input: %v", iter, err)
			os.Exit(1)
		}

		var nextInput string
		mapStart := time.Now()
		if err := runPageRankMappers(config, iterDir, planPath); err != nil {
			common.LogError("MASTER", "Iteration %d: map phase failed: %v", iter, err)
			os.Exit(1)
		}
		iterMetrics.MapTimeMs = time.Since(mapStart).Milliseconds()

		shuffleStart := time.Now()
		if err := commonmaster.Shuffle(config.M, config.R, iterDir); err != nil {
			common.LogError("MASTER", "Iteration %d: shuffle failed: %v", iter, err)
			os.Exit(1)
		}
		iterMetrics.ShuffleTimeMs = time.Since(shuffleStart).Milliseconds()

		reduceStart := time.Now()
		if err := runPageRankReducers(config, iterDir); err != nil {
			common.LogError("MASTER", "Iteration %d: reduce phase failed: %v", iter, err)
			os.Exit(1)
		}
		iterMetrics.ReduceTimeMs = time.Since(reduceStart).Milliseconds()

		mergeStart := time.Now()
		nextInput = filepath.Join(iterDir, "next_input.jsonl")
		if config.Mode == "mitigation" {
			if err := runPageRankMergeUnsalt(config, iterDir, nextInput); err != nil {
				common.LogError("MASTER", "Iteration %d: merge unsalt failed: %v", iter, err)
				os.Exit(1)
			}
		} else {
			if err := mergePageRankOutputs(config, iterDir, nextInput); err != nil {
				common.LogError("MASTER", "Iteration %d: merge failed: %v", iter, err)
				os.Exit(1)
			}
		}
		iterMetrics.MergeTimeMs = time.Since(mergeStart).Milliseconds()

		collectPageRankIterationMetrics(config, iterDir, &iterMetrics)

		iterElapsed := time.Since(iterStart)
		iterMetrics.TotalTimeMs = iterElapsed.Milliseconds()
		summary.IterationTimes[iterKey] = iterElapsed.Milliseconds()
		summary.IterationMetrics[iterKey] = iterMetrics

		currentInput = nextInput
		common.LogInfo("MASTER", "Iteration %d completed: elapsed=%dms", iter, iterElapsed.Milliseconds())
	}

	finalDir := filepath.Join(runPath, "final")
	if err := os.MkdirAll(finalDir, 0755); err != nil {
		common.LogError("MASTER", "Failed to create final directory: %v", err)
		os.Exit(1)
	}

	finalRanksPath := filepath.Join(finalDir, "ranks.jsonl")
	if err := commonmaster.CopyFile(currentInput, finalRanksPath); err != nil {
		common.LogError("MASTER", "Failed to write final ranks: %v", err)
		os.Exit(1)
	}

	if err := writeSortedRanks(currentInput, filepath.Join(finalDir, "ranks_sorted.jsonl")); err != nil {
		common.LogWarn("MASTER", "Failed to write sorted ranks: %v", err)
	}

	totalTime := time.Since(startTime)
	summary.TotalTimeMs = totalTime.Milliseconds()

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
func shardPageRankInput(inputPath string, iterDir string, M int) error {
	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	shardsDir := filepath.Join(iterDir, "shards")
	if err := os.MkdirAll(shardsDir, 0755); err != nil {
		return fmt.Errorf("failed to create shards directory: %w", err)
	}

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

	scanner := bufio.NewScanner(file)
	shardIdx := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		if _, err := shardWriters[shardIdx].Write(line); err != nil {
			return fmt.Errorf("failed to write to shard: %w", err)
		}
		if _, err := shardWriters[shardIdx].WriteString("\n"); err != nil {
			return fmt.Errorf("failed to write newline: %w", err)
		}
		shardIdx = (shardIdx + 1) % M
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input: %w", err)
	}

	return nil
}

// runPageRankMappersSample runs mappers in sample mode to detect heavy destination nodes
func runPageRankMappersSample(config PageRankConfig, runPath string) error {
	common.LogInfo("MASTER", "Running PageRank mappers in sample mode")

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

			shardPath := filepath.Join(runPath, "iter_000", "shards", fmt.Sprintf("shard_%03d.jsonl", mapID))
			outDir := filepath.Join(runPath, "iter_000", "intermediate", fmt.Sprintf("map_%d", mapID))

			cmd := commonmaster.BuildMapperCommand("sample", mapID, shardPath, outDir, config.SampleRate, 0, "", config.Seed, "pagerank")
			if err := commonmaster.RunCommand(cmd, filepath.Join(runPath, "logs", fmt.Sprintf("mapper_%d_sample.log", mapID))); err != nil {
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

// runPageRankMappers runs mappers for a PageRank iteration
func runPageRankMappers(config PageRankConfig, iterDir string, planPath string) error {
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

			cmd := commonmaster.BuildMapperCommand("execute", mapID, shardPath, outDir, 0, config.R, planPath, config.Seed, "pagerank")
			if err := commonmaster.RunCommand(cmd, filepath.Join(iterDir, "..", "..", "logs", fmt.Sprintf("mapper_%d_execute.log", mapID))); err != nil {
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

			cmd := commonmaster.BuildReducerCommand(reduceID, inputsPath, outPath, "pagerank", config.Damping, config.NumNodes)
			if err := commonmaster.RunCommand(cmd, filepath.Join(iterDir, "..", "..", "logs", fmt.Sprintf("reducer_%d.log", reduceID))); err != nil {
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
	common.LogInfo("MASTER", "Merging PageRank reducer outputs (baseline)")

	outFile, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	encoder := json.NewEncoder(outFile)

	for r := 0; r < config.R; r++ {
		reducePath := filepath.Join(iterDir, "output", fmt.Sprintf("reduce_%d.jsonl", r))
		file, err := os.Open(reducePath)
		if err != nil {
			continue
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}

			var jsonStr string
			if err := json.Unmarshal(line, &jsonStr); err != nil {
				file.Close()
				return fmt.Errorf("failed to unmarshal reducer output: %w", err)
			}

			inputRecord := common.InputRecord{Text: jsonStr}
			if err := encoder.Encode(inputRecord); err != nil {
				file.Close()
				return fmt.Errorf("failed to encode input record: %w", err)
			}
		}
		file.Close()

		if err := scanner.Err(); err != nil {
			return fmt.Errorf("error reading reducer output: %w", err)
		}
	}

	return nil
}

// runPageRankMergeUnsalt runs merge_unsalt to combine salted keys back to original keys
func runPageRankMergeUnsalt(config PageRankConfig, iterDir string, outPath string) error {
	common.LogInfo("MASTER", "Running merge_unsalt for PageRank")

	inputsGlob := filepath.Join(iterDir, "output", "reduce_*.jsonl")

	var cmd *exec.Cmd
	if _, err := os.Stat("bin/merge_unsalt_pagerank"); err == nil {
		cmd = exec.Command("bin/merge_unsalt_pagerank",
			"--inputs-glob", inputsGlob,
			"--out", outPath,
			"--damping", fmt.Sprintf("%.2f", config.Damping),
			"--num-nodes", strconv.Itoa(config.NumNodes),
		)
	} else {
		cmd = exec.Command("go", "run", "./pagerank/cmd/merge_unsalt",
			"--inputs-glob", inputsGlob,
			"--out", outPath,
			"--damping", fmt.Sprintf("%.2f", config.Damping),
			"--num-nodes", strconv.Itoa(config.NumNodes),
		)
	}

	return commonmaster.RunCommand(cmd, filepath.Join(iterDir, "..", "..", "logs", "merge_unsalt.log"))
}

// writeSortedRanks writes final ranks sorted by descending rank
func writeSortedRanks(inputPath string, outPath string) error {
	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input: %w", err)
	}
	defer file.Close()

	var records []pagerankjobs.PageRankNodeRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var inputRecord common.InputRecord
		if err := json.Unmarshal(line, &inputRecord); err == nil && inputRecord.Text != "" {
			var record pagerankjobs.PageRankNodeRecord
			if err := json.Unmarshal([]byte(inputRecord.Text), &record); err != nil {
				continue
			}
			records = append(records, record)
		} else {
			var record pagerankjobs.PageRankNodeRecord
			if err := json.Unmarshal(line, &record); err != nil {
				continue
			}
			records = append(records, record)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input: %w", err)
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].Rank > records[j].Rank
	})

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

// collectPageRankIterationMetrics reads reducer stats and computes load balancing metrics
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

// findOriginalGraphFile finds the original edge list file from graph state path
func findOriginalGraphFile(graphStatePath string) string {
	baseDir := filepath.Dir(graphStatePath)
	baseName := filepath.Base(graphStatePath)

	candidates := []string{
		filepath.Join(baseDir, strings.TrimSuffix(baseName, "_init.jsonl")+".txt"),
		filepath.Join(baseDir, strings.TrimSuffix(baseName, "_init.jsonl")),
		filepath.Join(baseDir, strings.TrimSuffix(baseName, ".jsonl")+".txt"),
		filepath.Join(baseDir, strings.TrimSuffix(baseName, ".jsonl")),
		filepath.Join(baseDir, "graph.txt"),
		filepath.Join(baseDir, "graph_zipf.txt"),
		filepath.Join(baseDir, "graph_skewed.txt"),
		filepath.Join(baseDir, "test_graph.txt"),
	}

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

// createPageRankPartitionPlan analyzes sample data and creates a partition plan
func createPageRankPartitionPlan(config PageRankConfig, runPath string) (string, error) {
	common.LogInfo("MASTER", "Creating partition plan for PageRank")

	aggregateCounts := make(map[string]int)
	for m := 0; m < config.M; m++ {
		samplePath := filepath.Join(runPath, "iter_000", "intermediate", fmt.Sprintf("map_%d", m), "sample_counts.json")
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

	totalSamples := 0
	for _, count := range aggregateCounts {
		totalSamples += count
	}

	threshold := int(float64(totalSamples) * config.HeavyTopPct)
	plan := &common.PartitionPlan{
		R:     config.R,
		Heavy: make(map[string]common.HeavyKeyInfo),
	}

	for key, count := range aggregateCounts {
		if count >= threshold {
			plan.Heavy[key] = common.HeavyKeyInfo{
				Splits: config.FixedSplits,
			}
			common.LogInfo("MASTER", "Identified heavy destination node: %s (sample_count: %d, splits: %d)", key, count, config.FixedSplits)
		}
	}

	planPath := filepath.Join(runPath, "partition_plan.json")
	planFile, err := os.Create(planPath)
	if err != nil {
		return "", fmt.Errorf("failed to create plan file: %w", err)
	}
	defer planFile.Close()

	encoder := json.NewEncoder(planFile)
	if err := encoder.Encode(plan); err != nil {
		return "", fmt.Errorf("failed to encode plan: %w", err)
	}

	common.LogInfo("MASTER", "Partition plan created: %d heavy nodes", len(plan.Heavy))
	return planPath, nil
}

// reconstructEdgeListFromGraphState reconstructs edge list from graph state
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

		var nodeRecord pagerankjobs.PageRankNodeRecord
		if err := json.Unmarshal(line, &nodeRecord); err != nil {
			continue
		}

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
