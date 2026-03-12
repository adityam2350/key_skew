package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"

	"key_skew/common/common"
	commonjobs "key_skew/common/jobs"
	_ "key_skew/pagerank/jobs"
	_ "key_skew/wordcount/jobs"
)

func main() {
	var mode string
	var mapID int
	var shardPath string
	var outDir string
	var jobName string
	var sampleRate float64
	var seed int64
	var R int
	var planPath string

	flag.StringVar(&mode, "mode", "execute", "Mode: sample or execute")
	flag.IntVar(&mapID, "map-id", 0, "Mapper ID")
	flag.StringVar(&shardPath, "shard", "", "Input shard file path")
	flag.StringVar(&outDir, "outdir", "", "Output directory")
	flag.StringVar(&jobName, "job", "", "Job name (wordcount or pagerank)")
	flag.Float64Var(&sampleRate, "sample-rate", 0.01, "Sampling rate (sample mode only)")
	flag.Int64Var(&seed, "seed", 0, "Random seed")
	flag.IntVar(&R, "R", 8, "Number of reducers (execute mode only)")
	flag.StringVar(&planPath, "plan", "", "Partition plan path (execute mode only)")
	flag.Parse()

	if shardPath == "" || outDir == "" || jobName == "" {
		fmt.Fprintf(os.Stderr, "Error: --shard, --outdir, and --job are required\n")
		os.Exit(1)
	}

	registry := commonjobs.NewJobRegistry()
	job, ok := registry.Get(jobName)
	if !ok {
		fmt.Fprintf(os.Stderr, "Error: Unknown job: %s\n", jobName)
		os.Exit(1)
	}

	var plan *common.PartitionPlan
	if planPath != "" {
		if file, err := os.Open(planPath); err == nil {
			decoder := json.NewDecoder(file)
			if err := decoder.Decode(&plan); err != nil {
				fmt.Fprintf(os.Stderr, "Error: Failed to decode partition plan: %v\n", err)
				os.Exit(1)
			}
			file.Close()
		}
	}

	params := make(map[string]interface{})
	if planPath != "" {
		params["plan_path"] = planPath
	}
	if err := job.SetParameters(params); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to set job parameters: %v\n", err)
		os.Exit(1)
	}

	rand.Seed(seed)

	shardFile, err := os.Open(shardPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to open shard: %v\n", err)
		os.Exit(1)
	}
	defer shardFile.Close()

	if err := os.MkdirAll(outDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create output directory: %v\n", err)
		os.Exit(1)
	}

	if mode == "sample" {
		runSampleMode(shardFile, outDir, job, sampleRate, rand.New(rand.NewSource(seed)))
	} else {
		runExecuteMode(shardFile, outDir, job, R, plan)
	}
}

// runSampleMode runs mapper in sample mode to collect key frequencies
func runSampleMode(shardFile *os.File, outDir string, job commonjobs.Job, sampleRate float64, rng *rand.Rand) {
	scanner := bufio.NewScanner(shardFile)
	counts := make(map[string]int)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		if rng.Float64() >= sampleRate {
			continue
		}

		var record common.InputRecord
		if err := json.Unmarshal(line, &record); err != nil {
			continue
		}

		kvs := job.Map(record)
		for _, kv := range kvs {
			keyStr := fmt.Sprintf("%v", kv.K)
			counts[keyStr]++
		}
	}

	samplePath := filepath.Join(outDir, "sample_counts.json")
	sampleFile, err := os.Create(samplePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create sample file: %v\n", err)
		os.Exit(1)
	}
	defer sampleFile.Close()

	encoder := json.NewEncoder(sampleFile)
	if err := encoder.Encode(counts); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to encode sample counts: %v\n", err)
		os.Exit(1)
	}
}

// runExecuteMode runs mapper in execute mode to process records and partition output
func runExecuteMode(shardFile *os.File, outDir string, job commonjobs.Job, R int, plan *common.PartitionPlan) {
	scanner := bufio.NewScanner(shardFile)

	partFiles := make([]*os.File, R)
	encoders := make([]*json.Encoder, R)
	for r := 0; r < R; r++ {
		partPath := filepath.Join(outDir, fmt.Sprintf("part_%03d.jsonl", r))
		partFile, err := os.Create(partPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Failed to create partition file: %v\n", err)
			os.Exit(1)
		}
		partFiles[r] = partFile
		encoders[r] = json.NewEncoder(partFile)
	}
	defer func() {
		for _, f := range partFiles {
			if f != nil {
				f.Close()
			}
		}
	}()

	var recordsProcessed int64
	var kvsEmitted int64

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var record common.InputRecord
		if err := json.Unmarshal(line, &record); err != nil {
			continue
		}

		recordsProcessed++

		kvs := job.Map(record)
		kvsEmitted += int64(len(kvs))

		for _, kv := range kvs {
			key := kv.K

			partition, err := common.PartitionKey(key, R)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: Failed to partition key: %v\n", err)
				continue
			}

			outputKV := common.KV{K: key, V: kv.V}
			if err := encoders[partition].Encode(outputKV); err != nil {
				fmt.Fprintf(os.Stderr, "Error: Failed to encode KV: %v\n", err)
				continue
			}
		}
	}

	statsPath := filepath.Join(outDir, "map_stats.json")
	statsFile, err := os.Create(statsPath)
	if err == nil {
		stats := map[string]interface{}{
			"records_processed": recordsProcessed,
			"kvs_emitted":       kvsEmitted,
		}
		encoder := json.NewEncoder(statsFile)
		encoder.Encode(stats)
		statsFile.Close()
	}
}
