package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"key_skew/common/common"
	pagerankjobs "key_skew/pagerank/jobs"
)

func main() {
	var inputsGlob string
	var outPath string
	var damping float64
	var numNodes int

	flag.StringVar(&inputsGlob, "inputs-glob", "", "Glob pattern for reducer output files")
	flag.StringVar(&outPath, "out", "", "Output JSONL file path")
	flag.Float64Var(&damping, "damping", 0.85, "PageRank damping factor")
	flag.IntVar(&numNodes, "num-nodes", 0, "Number of nodes in graph")
	flag.Parse()

	if numNodes == 0 {
		fmt.Fprintf(os.Stderr, "Error: --num-nodes is required\n")
		os.Exit(1)
	}

	logDir := filepath.Join(filepath.Dir(outPath), "..", "logs")
	if err := common.InitLogging(logDir, "merge_unsalt"); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logging: %v\n", err)
		os.Exit(1)
	}
	defer common.CloseLogging()

	startTime := time.Now()
	common.LogInfo("MERGE_UNSALT", "Starting merge: glob=%s, out=%s", inputsGlob, outPath)

	matches, err := filepath.Glob(inputsGlob)
	if err != nil {
		common.LogError("MERGE_UNSALT", "Failed to glob pattern: %v", err)
		os.Exit(1)
	}

	if len(matches) == 0 {
		common.LogWarn("MERGE_UNSALT", "No files matched glob pattern: %s", inputsGlob)
	}

	contributions := make(map[string]float64)
	finalRanks := make(map[string]float64)
	adjacencyLists := make(map[string][]string)
	hasSaltedContributions := make(map[string]bool)
	var recordsRead int64

	for _, inputFile := range matches {
		file, err := os.Open(inputFile)
		if err != nil {
			common.LogWarn("MERGE_UNSALT", "Failed to open %s: %v", inputFile, err)
			continue
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			recordsRead++
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}

			var jsonStr string
			if err := json.Unmarshal(line, &jsonStr); err != nil {
				common.LogWarn("MERGE_UNSALT", "Failed to parse JSON string: %v", err)
				continue
			}

			var nodeRecord pagerankjobs.PageRankNodeRecord
			if err := json.Unmarshal([]byte(jsonStr), &nodeRecord); err != nil {
				common.LogWarn("MERGE_UNSALT", "Failed to parse node record: %v", err)
				continue
			}

			keyStr := nodeRecord.Node
			baseKey := keyStr
			isSalted := strings.Contains(keyStr, "#")

			if isSalted {
				parts := strings.Split(keyStr, "#")
				if len(parts) > 0 {
					baseKey = parts[0]
				}
				contributions[baseKey] += nodeRecord.Rank
				hasSaltedContributions[baseKey] = true
			} else {
				if len(nodeRecord.Neighbors) > 0 {
					adjacencyLists[baseKey] = nodeRecord.Neighbors
				}
				finalRanks[baseKey] = nodeRecord.Rank
			}
		}

		file.Close()
		if err := scanner.Err(); err != nil {
			common.LogError("MERGE_UNSALT", "Error reading %s: %v", inputFile, err)
		}
	}

	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		common.LogError("MERGE_UNSALT", "Failed to create output directory: %v", err)
		os.Exit(1)
	}

	outFile, err := os.Create(outPath)
	if err != nil {
		common.LogError("MERGE_UNSALT", "Failed to create output file: %v", err)
		os.Exit(1)
	}
	defer outFile.Close()

	encoder := json.NewEncoder(outFile)

	allKeys := make(map[string]bool)
	for baseKey := range contributions {
		allKeys[baseKey] = true
	}
	for baseKey := range finalRanks {
		allKeys[baseKey] = true
	}

	for baseKey := range allKeys {
		neighbors := adjacencyLists[baseKey]
		if neighbors == nil {
			neighbors = []string{}
		}

		var finalRank float64
		if hasSaltedContributions[baseKey] {
			totalContribution := contributions[baseKey]
			finalRank = (1.0-damping)/float64(numNodes) + damping*totalContribution
		} else {
			finalRank = finalRanks[baseKey]
		}

		nodeRecord := pagerankjobs.PageRankNodeRecord{
			Node:      baseKey,
			Rank:      finalRank,
			Neighbors: neighbors,
		}

		if err := encoder.Encode(nodeRecord); err != nil {
			common.LogError("MERGE_UNSALT", "Failed to encode output record: %v", err)
			os.Exit(1)
		}
	}

	elapsed := time.Since(startTime)
	common.LogInfo("MERGE_UNSALT", "Merge completed: records_read=%d, unique_keys=%d, elapsed=%dms",
		recordsRead, len(contributions), elapsed.Milliseconds())
}
