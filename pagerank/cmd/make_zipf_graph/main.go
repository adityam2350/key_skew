package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
)

type GraphMetadata struct {
	NumNodes    int            `json:"num_nodes"`
	NumEdges    int            `json:"num_edges"`
	ZipfS       float64        `json:"zipf_s"`
	ZipfV       float64        `json:"zipf_v"`
	Seed        int64          `json:"seed"`
	AvgOutDeg   float64        `json:"avg_outdegree"`
	TopDestFreq map[string]int `json:"top_dest_freq"`
}

func main() {
	var numNodes int
	var numEdges int
	var zipfS float64
	var zipfV float64
	var seed int64
	var outPath string

	flag.IntVar(&numNodes, "num-nodes", 1000, "Number of nodes in the graph")
	flag.IntVar(&numEdges, "num-edges", 10000, "Number of edges in the graph")
	flag.Float64Var(&zipfS, "zipf-s", 1.1, "Zipf distribution parameter s (higher = more skewed)")
	flag.Float64Var(&zipfV, "zipf-v", 1.0, "Zipf distribution parameter v")
	flag.Int64Var(&seed, "seed", 0, "Random seed for deterministic generation")
	flag.StringVar(&outPath, "out", "", "Output edge list file path")
	flag.Parse()

	if outPath == "" {
		fmt.Fprintf(os.Stderr, "Error: --out is required\n")
		os.Exit(1)
	}

	rng := rand.New(rand.NewSource(seed))

	if zipfS <= 1.0 {
		fmt.Fprintf(os.Stderr, "Error: zipf-s must be > 1.0 (got %.2f)\n", zipfS)
		os.Exit(1)
	}
	if zipfV <= 0 {
		fmt.Fprintf(os.Stderr, "Error: zipf-v must be > 0 (got %.2f)\n", zipfV)
		os.Exit(1)
	}
	if numNodes < 2 {
		fmt.Fprintf(os.Stderr, "Error: num-nodes must be >= 2 (got %d)\n", numNodes)
		os.Exit(1)
	}

	probabilities := make([]float64, numNodes)
	var sum float64
	for i := 0; i < numNodes; i++ {
		prob := 1.0 / math.Pow(float64(i+1), zipfS)
		probabilities[i] = prob
		sum += prob
	}
	for i := 0; i < numNodes; i++ {
		probabilities[i] /= sum
	}
	cumulative := make([]float64, numNodes)
	cumulative[0] = probabilities[0]
	for i := 1; i < numNodes; i++ {
		cumulative[i] = cumulative[i-1] + probabilities[i]
	}

	destCounts := make(map[string]int)
	edges := make(map[string]map[string]bool)

	outFile, err := os.Create(outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create output file: %v\n", err)
		os.Exit(1)
	}
	defer outFile.Close()

	writer := bufio.NewWriter(outFile)
	defer writer.Flush()

	edgesWritten := 0

	for edgesWritten < numEdges {
		srcIdx := rng.Intn(numNodes)
		srcNode := fmt.Sprintf("n%06d", srcIdx+1)

		u := rng.Float64()
		dstIdx := 0
		for i := 0; i < numNodes-1; i++ {
			if u < cumulative[i] {
				dstIdx = i
				break
			}
		}
		if u >= cumulative[numNodes-1] {
			dstIdx = numNodes - 1
		}
		dstNode := fmt.Sprintf("n%06d", dstIdx+1)

		if srcNode == dstNode {
			continue
		}

		if edges[srcNode] == nil {
			edges[srcNode] = make(map[string]bool)
		}
		if edges[srcNode][dstNode] {
			continue
		}
		edges[srcNode][dstNode] = true

		line := fmt.Sprintf("%s %s\n", srcNode, dstNode)
		if _, err := writer.WriteString(line); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write edge: %v\n", err)
			os.Exit(1)
		}

		destCounts[dstNode]++
		edgesWritten++
	}

	avgOutDeg := float64(edgesWritten) / float64(numNodes)

	type destFreq struct {
		node string
		freq int
	}
	topDests := make([]destFreq, 0, len(destCounts))
	for node, count := range destCounts {
		topDests = append(topDests, destFreq{node: node, freq: count})
	}
	sort.Slice(topDests, func(i, j int) bool {
		return topDests[i].freq > topDests[j].freq
	})

	topDestFreq := make(map[string]int)
	for i := 0; i < 10 && i < len(topDests); i++ {
		topDestFreq[topDests[i].node] = topDests[i].freq
	}

	metaPath := outPath + ".meta.json"
	meta := GraphMetadata{
		NumNodes:    numNodes,
		NumEdges:    edgesWritten,
		ZipfS:       zipfS,
		ZipfV:       zipfV,
		Seed:        seed,
		AvgOutDeg:   avgOutDeg,
		TopDestFreq: topDestFreq,
	}

	metaFile, err := os.Create(metaPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create metadata file: %v\n", err)
		os.Exit(1)
	}
	defer metaFile.Close()

	encoder := json.NewEncoder(metaFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(meta); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to encode metadata: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Generated Zipf-like graph:\n")
	fmt.Printf("  Nodes: %d\n", numNodes)
	fmt.Printf("  Edges: %d\n", edgesWritten)
	fmt.Printf("  Avg outdegree: %.2f\n", avgOutDeg)
	fmt.Printf("  Top destination: %s (in-degree: %d)\n", topDests[0].node, topDests[0].freq)
	fmt.Printf("  Output: %s\n", outPath)
	fmt.Printf("  Metadata: %s\n", metaPath)
}
