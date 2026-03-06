package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// PageRankNodeRecord represents a node's state in the graph
// This format is used as input/output for each PageRank iteration
type PageRankNodeRecord struct {
	Node      string   `json:"node"`
	Rank      float64  `json:"rank"`
	Neighbors []string `json:"neighbors"`
}

// GraphInitMetadata contains metadata about the initialized graph
type GraphInitMetadata struct {
	NumNodes          int     `json:"num_nodes"`
	Damping           float64 `json:"damping"`
	DefaultIterations int     `json:"default_iterations"`
	SourceInputPath   string  `json:"source_input_path"`
}

func main() {
	var inputPath string
	var outPath string

	flag.StringVar(&inputPath, "input", "", "Input edge list file path")
	flag.StringVar(&outPath, "out", "", "Output graph state JSONL file path")
	flag.Parse()

	if inputPath == "" {
		fmt.Fprintf(os.Stderr, "Error: --input is required\n")
		os.Exit(1)
	}
	if outPath == "" {
		fmt.Fprintf(os.Stderr, "Error: --out is required\n")
		os.Exit(1)
	}

	// Read edge list
	file, err := os.Open(inputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open input file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// Build adjacency lists
	// Map from node -> list of outgoing neighbors
	adjacencyLists := make(map[string][]string)
	allNodes := make(map[string]bool)

	scanner := bufio.NewScanner(file)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if len(line) == 0 {
			continue
		}

		// Parse edge: "src dst"
		parts := strings.Fields(line)
		if len(parts) < 2 {
			fmt.Fprintf(os.Stderr, "Warning: skipping malformed line %d: %s\n", lineNum, line)
			continue
		}

		src := parts[0]
		dst := parts[1]

		allNodes[src] = true
		allNodes[dst] = true

		// Add edge to adjacency list
		adjacencyLists[src] = append(adjacencyLists[src], dst)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input file: %v\n", err)
		os.Exit(1)
	}

	// Count total nodes
	numNodes := len(allNodes)
	if numNodes == 0 {
		fmt.Fprintf(os.Stderr, "Error: no nodes found in input file\n")
		os.Exit(1)
	}

	// Initialize rank: 1.0 / N
	initialRank := 1.0 / float64(numNodes)

	// Create output directory
	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create output directory: %v\n", err)
		os.Exit(1)
	}

	// Write graph state JSONL
	outFile, err := os.Create(outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create output file: %v\n", err)
		os.Exit(1)
	}
	defer outFile.Close()

	encoder := json.NewEncoder(outFile)

	// Write one record per node
	// Ensure all nodes are included, even if they have no outgoing edges
	for node := range allNodes {
		neighbors := adjacencyLists[node]
		if neighbors == nil {
			neighbors = []string{} // Empty list for nodes with no outgoing edges
		}

		record := PageRankNodeRecord{
			Node:      node,
			Rank:      initialRank,
			Neighbors: neighbors,
		}

		if err := encoder.Encode(record); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to encode record: %v\n", err)
			os.Exit(1)
		}
	}

	// Write metadata file
	metaPath := outPath + ".meta.json"
	meta := GraphInitMetadata{
		NumNodes:          numNodes,
		Damping:           0.85,
		DefaultIterations: 10,
		SourceInputPath:   inputPath,
	}

	metaFile, err := os.Create(metaPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create metadata file: %v\n", err)
		os.Exit(1)
	}
	defer metaFile.Close()

	metaEncoder := json.NewEncoder(metaFile)
	metaEncoder.SetIndent("", "  ")
	if err := metaEncoder.Encode(meta); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to encode metadata: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Initialized PageRank graph:\n")
	fmt.Printf("  Nodes: %d\n", numNodes)
	fmt.Printf("  Initial rank: %.6f\n", initialRank)
	fmt.Printf("  Output: %s\n", outPath)
	fmt.Printf("  Metadata: %s\n", metaPath)
}
