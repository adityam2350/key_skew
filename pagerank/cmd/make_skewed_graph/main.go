package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
)

type SkewedGraphMetadata struct {
	NumNodes     int            `json:"num_nodes"`
	NumEdges     int            `json:"num_edges"`
	NumHotNodes  int            `json:"num_hot_nodes"`
	HotEdgeFrac  float64        `json:"hot_edge_frac"`
	Seed         int64          `json:"seed"`
	HotNodeEdges int            `json:"hot_node_edges"`
	OtherEdges   int            `json:"other_edges"`
	HotNodeFreq  map[string]int `json:"hot_node_freq"`
}

func main() {
	var numNodes int
	var numEdges int
	var numHotNodes int
	var hotEdgeFrac float64
	var seed int64
	var outPath string

	flag.IntVar(&numNodes, "num-nodes", 1000, "Number of nodes in the graph")
	flag.IntVar(&numEdges, "num-edges", 10000, "Number of edges in the graph")
	flag.IntVar(&numHotNodes, "num-hot-nodes", 3, "Number of hot nodes (first K nodes)")
	flag.Float64Var(&hotEdgeFrac, "hot-edge-frac", 0.80, "Fraction of edges targeting hot nodes")
	flag.Int64Var(&seed, "seed", 0, "Random seed for deterministic generation")
	flag.StringVar(&outPath, "out", "", "Output edge list file path")
	flag.Parse()

	if outPath == "" {
		fmt.Fprintf(os.Stderr, "Error: --out is required\n")
		os.Exit(1)
	}

	if numHotNodes > numNodes {
		fmt.Fprintf(os.Stderr, "Error: num-hot-nodes (%d) cannot exceed num-nodes (%d)\n", numHotNodes, numNodes)
		os.Exit(1)
	}

	rng := rand.New(rand.NewSource(seed))

	hotNodes := make(map[string]bool)
	for i := 0; i < numHotNodes; i++ {
		hotNode := fmt.Sprintf("n%06d", i+1)
		hotNodes[hotNode] = true
	}

	hotEdges := int(float64(numEdges) * hotEdgeFrac)
	otherEdges := numEdges - hotEdges

	edges := make(map[string]map[string]bool)
	hotNodeFreq := make(map[string]int)

	outFile, err := os.Create(outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create output file: %v\n", err)
		os.Exit(1)
	}
	defer outFile.Close()

	writer := bufio.NewWriter(outFile)
	defer writer.Flush()

	edgesWritten := 0

	for edgesWritten < hotEdges {
		srcIdx := rng.Intn(numNodes)
		srcNode := fmt.Sprintf("n%06d", srcIdx+1)

		hotIdx := rng.Intn(numHotNodes)
		dstNode := fmt.Sprintf("n%06d", hotIdx+1)

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

		hotNodeFreq[dstNode]++
		edgesWritten++
	}

	for edgesWritten < numEdges {
		srcIdx := rng.Intn(numNodes)
		srcNode := fmt.Sprintf("n%06d", srcIdx+1)

		dstIdx := rng.Intn(numNodes)
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

		edgesWritten++
	}

	metaPath := outPath + ".meta.json"
	meta := SkewedGraphMetadata{
		NumNodes:     numNodes,
		NumEdges:     edgesWritten,
		NumHotNodes:  numHotNodes,
		HotEdgeFrac:  hotEdgeFrac,
		Seed:         seed,
		HotNodeEdges: hotEdges,
		OtherEdges:   otherEdges,
		HotNodeFreq:  hotNodeFreq,
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

	fmt.Printf("Generated heavily skewed graph:\n")
	fmt.Printf("  Nodes: %d\n", numNodes)
	fmt.Printf("  Edges: %d\n", edgesWritten)
	fmt.Printf("  Hot nodes: %d (first %d nodes)\n", numHotNodes, numHotNodes)
	fmt.Printf("  Hot edges: %d (%.1f%%)\n", hotEdges, hotEdgeFrac*100)
	fmt.Printf("  Other edges: %d\n", otherEdges)

	type hotFreq struct {
		node string
		freq int
	}
	hotList := make([]hotFreq, 0, len(hotNodeFreq))
	for node, count := range hotNodeFreq {
		hotList = append(hotList, hotFreq{node: node, freq: count})
	}
	sort.Slice(hotList, func(i, j int) bool {
		return hotList[i].freq > hotList[j].freq
	})

	if len(hotList) > 0 {
		fmt.Printf("  Top hot node: %s (in-degree: %d)\n", hotList[0].node, hotList[0].freq)
	}

	fmt.Printf("  Output: %s\n", outPath)
	fmt.Printf("  Metadata: %s\n", metaPath)
}
