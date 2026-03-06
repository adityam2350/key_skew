package jobs

import (
	"encoding/json"
	"fmt"

	"key_skew/internal/common"
)

// PageRankNodeRecord represents a node's state in the graph
// This is the input format for each PageRank iteration
type PageRankNodeRecord struct {
	Node      string   `json:"node"`
	Rank      float64  `json:"rank"`
	Neighbors []string `json:"neighbors"`
}

// PageRankValue represents a value in the PageRank MapReduce pipeline
// It can be either an adjacency message or a contribution message
type PageRankValue struct {
	Type      string   `json:"type"` // "adjacency" or "contribution"
	Neighbors []string `json:"neighbors,omitempty"`
	Amount    float64  `json:"amount,omitempty"`
}

// MapRecordPageRank processes a PageRank node record and emits key-value pairs
// For each node, it emits:
// 1. An adjacency record for the node itself (to preserve graph structure)
// 2. A contribution record for each outgoing neighbor
//
// Why adjacency lists must be preserved:
// PageRank is iterative - each round needs the graph structure to compute
// the next iteration. Reducers must reconstruct each node's adjacency list
// so the next iteration can distribute rank contributions correctly.
func MapRecordPageRank(record common.InputRecord) []common.KV {
	// Parse the node record
	var nodeRecord PageRankNodeRecord
	if err := json.Unmarshal([]byte(record.Text), &nodeRecord); err != nil {
		// If parsing fails, return empty (error handling at framework level)
		return []common.KV{}
	}

	kvs := make([]common.KV, 0)

	// Emit adjacency record for the node itself
	// This ensures the reducer knows the graph structure for the next iteration
	adjValue := PageRankValue{
		Type:      "adjacency",
		Neighbors: nodeRecord.Neighbors,
	}

	// Serialize as JSON string for the value
	adjJSON, err := json.Marshal(adjValue)
	if err != nil {
		return []common.KV{} // Error handling
	}

	kvs = append(kvs, common.KV{
		K: nodeRecord.Node,
		V: string(adjJSON), // Store as string for now, will be parsed in reducer
	})

	// Emit contribution records for each outgoing neighbor
	// Each neighbor receives rank / outdegree
	outdegree := len(nodeRecord.Neighbors)
	if outdegree > 0 {
		contribution := nodeRecord.Rank / float64(outdegree)

		for _, neighbor := range nodeRecord.Neighbors {
			contribValue := PageRankValue{
				Type:   "contribution",
				Amount: contribution,
			}

			contribJSON, err := json.Marshal(contribValue)
			if err != nil {
				continue // Skip on error
			}

			kvs = append(kvs, common.KV{
				K: neighbor,
				V: string(contribJSON),
			})
		}
	}

	// Note: Dangling nodes (nodes with zero outgoing edges) are handled:
	// - They emit only an adjacency record (empty neighbors list)
	// - They receive contributions but don't distribute any
	// - For MVP, we do NOT redistribute dangling mass (this is a known limitation)
	// - In a full implementation, dangling mass would be distributed uniformly

	return kvs
}

// ReduceKeyPageRank aggregates PageRank values for a node and computes new rank
// Input: node key and list of value strings (JSON-encoded PageRankValue)
// Output: JSON-encoded PageRankNodeRecord for the next iteration
//
// The reducer must:
// 1. Parse all values to separate adjacency messages from contributions
// 2. Recover the adjacency list (graph structure)
// 3. Sum all incoming contributions
// 4. Compute new rank: (1-damping)/N + damping * sum(contributions)
// 5. Emit updated node record for next iteration
func ReduceKeyPageRank(key string, values []string, damping float64, numNodes int) string {
	var adjacencyList []string
	var totalContribution float64

	// Parse all values
	for _, valueStr := range values {
		var prValue PageRankValue
		if err := json.Unmarshal([]byte(valueStr), &prValue); err != nil {
			continue // Skip malformed values
		}

		if prValue.Type == "adjacency" {
			// Recover graph structure
			adjacencyList = prValue.Neighbors
		} else if prValue.Type == "contribution" {
			// Accumulate rank contributions
			totalContribution += prValue.Amount
		}
	}

	// If no adjacency record was found, use empty list
	// This should not happen in correct execution, but handle gracefully
	if adjacencyList == nil {
		adjacencyList = []string{}
	}

	// Compute new rank using PageRank formula:
	// PR_new(v) = (1 - damping)/N + damping * sum(incoming contributions)
	// The (1-damping)/N term ensures all nodes get a minimum rank
	// The damping term scales the incoming contributions
	newRank := (1.0-damping)/float64(numNodes) + damping*totalContribution

	// Create updated node record
	nodeRecord := PageRankNodeRecord{
		Node:      key,
		Rank:      newRank,
		Neighbors: adjacencyList,
	}

	// Serialize to JSON string
	resultJSON, err := json.Marshal(nodeRecord)
	if err != nil {
		return "{}" // Error handling
	}

	return string(resultJSON)
}

// ParsePageRankNodeRecord parses a JSON string into a PageRankNodeRecord
// Used for reading graph state files
func ParsePageRankNodeRecord(jsonStr string) (*PageRankNodeRecord, error) {
	var record PageRankNodeRecord
	if err := json.Unmarshal([]byte(jsonStr), &record); err != nil {
		return nil, fmt.Errorf("failed to parse PageRank node record: %w", err)
	}
	return &record, nil
}
