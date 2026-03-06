package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"key_skew/common/common"
	"key_skew/pagerank/internal/heavy"
)

func main() {
	var inputPath string
	var outputPath string
	var topK int
	var defaultSplits int

	flag.StringVar(&inputPath, "input", "", "Input edge list file path (src dst format)")
	flag.StringVar(&outputPath, "out", "", "Output heavy config JSON file path")
	flag.IntVar(&topK, "top-k", 10, "Number of top heavy-hitter nodes to select")
	flag.IntVar(&defaultSplits, "default-splits", 8, "Default number of splits for heavy nodes")
	flag.Parse()

	if inputPath == "" {
		fmt.Fprintf(os.Stderr, "Error: --input is required\n")
		os.Exit(1)
	}

	if outputPath == "" {
		fmt.Fprintf(os.Stderr, "Error: --out is required\n")
		os.Exit(1)
	}

	if err := common.InitLogging(".", "analyze_heavy"); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logging: %v\n", err)
		os.Exit(1)
	}
	defer common.CloseLogging()

	common.LogInfo("ANALYZE_HEAVY", "Analyzing graph: input=%s, topK=%d, splits=%d", inputPath, topK, defaultSplits)

	inDegreeCounts := make(map[string]int)
	totalEdges := 0

	file, err := os.Open(inputPath)
	if err != nil {
		common.LogError("ANALYZE_HEAVY", "Failed to open input file: %v", err)
		os.Exit(1)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) >= 2 {
			destNode := parts[1]
			inDegreeCounts[destNode]++
			totalEdges++
		}
	}

	if err := scanner.Err(); err != nil {
		common.LogError("ANALYZE_HEAVY", "Error reading input file: %v", err)
		os.Exit(1)
	}

	type nodeInDegree struct {
		Node     string
		InDegree int
	}

	var nodes []nodeInDegree
	for node, count := range inDegreeCounts {
		nodes = append(nodes, nodeInDegree{Node: node, InDegree: count})
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].InDegree > nodes[j].InDegree
	})

	heavyConfig := &heavy.HeavyConfig{
		HeavyNodes: make(map[string]int),
	}

	selectedCount := topK
	if selectedCount > len(nodes) {
		selectedCount = len(nodes)
	}

	for i := 0; i < selectedCount; i++ {
		node := nodes[i].Node
		heavyConfig.HeavyNodes[node] = defaultSplits
		common.LogInfo("ANALYZE_HEAVY", "Selected heavy node: %s (in-degree: %d, splits: %d)", node, nodes[i].InDegree, defaultSplits)
	}

	if err := heavyConfig.Save(outputPath); err != nil {
		common.LogError("ANALYZE_HEAVY", "Failed to save heavy config: %v", err)
		os.Exit(1)
	}

	common.LogInfo("ANALYZE_HEAVY", "Generated heavy config: %d nodes, output=%s", len(heavyConfig.HeavyNodes), outputPath)
}
