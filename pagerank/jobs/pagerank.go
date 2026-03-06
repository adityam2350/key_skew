package jobs

import (
	"encoding/json"
	"fmt"
	"strings"

	"key_skew/common/common"
)

type PageRankNodeRecord struct {
	Node      string   `json:"node"`
	Rank      float64  `json:"rank"`
	Neighbors []string `json:"neighbors"`
}

type PageRankValue struct {
	Type      string   `json:"type"`
	Neighbors []string `json:"neighbors,omitempty"`
	Amount    float64  `json:"amount,omitempty"`
}

// MapRecordPageRank processes a PageRank node record and emits key-value pairs
func MapRecordPageRank(record common.InputRecord) []common.KV {
	var nodeRecord PageRankNodeRecord
	if err := json.Unmarshal([]byte(record.Text), &nodeRecord); err != nil {
		return []common.KV{}
	}

	kvs := make([]common.KV, 0)

	adjValue := PageRankValue{
		Type:      "adjacency",
		Neighbors: nodeRecord.Neighbors,
	}

	adjJSON, err := json.Marshal(adjValue)
	if err != nil {
		return []common.KV{}
	}

	kvs = append(kvs, common.KV{
		K: nodeRecord.Node,
		V: string(adjJSON),
	})

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
				continue
			}

			kvs = append(kvs, common.KV{
				K: neighbor,
				V: string(contribJSON),
			})
		}
	}

	return kvs
}

// ReduceKeyPageRank aggregates PageRank values for a node and computes new rank
func ReduceKeyPageRank(key string, values []string, damping float64, numNodes int) string {
	var adjacencyList []string
	var totalContribution float64

	isSalted := strings.Contains(key, "#")
	baseKey := key
	if isSalted {
		parts := strings.Split(key, "#")
		if len(parts) > 0 {
			baseKey = parts[0]
		}
	}

	for _, valueStr := range values {
		var prValue PageRankValue
		if err := json.Unmarshal([]byte(valueStr), &prValue); err != nil {
			continue
		}

		if prValue.Type == "adjacency" {
			if !isSalted {
				adjacencyList = prValue.Neighbors
			}
		} else if prValue.Type == "contribution" {
			totalContribution += prValue.Amount
		}
	}

	if adjacencyList == nil {
		adjacencyList = []string{}
	}

	if isSalted {
		nodeRecord := PageRankNodeRecord{
			Node:      key,
			Rank:      totalContribution,
			Neighbors: []string{},
		}
		resultJSON, err := json.Marshal(nodeRecord)
		if err != nil {
			return "{}"
		}
		return string(resultJSON)
	}

	newRank := (1.0-damping)/float64(numNodes) + damping*totalContribution

	nodeRecord := PageRankNodeRecord{
		Node:      baseKey,
		Rank:      newRank,
		Neighbors: adjacencyList,
	}

	resultJSON, err := json.Marshal(nodeRecord)
	if err != nil {
		return "{}"
	}

	return string(resultJSON)
}

// ParsePageRankNodeRecord parses a JSON string into a PageRankNodeRecord
func ParsePageRankNodeRecord(jsonStr string) (*PageRankNodeRecord, error) {
	var record PageRankNodeRecord
	if err := json.Unmarshal([]byte(jsonStr), &record); err != nil {
		return nil, fmt.Errorf("failed to parse PageRank node record: %w", err)
	}
	return &record, nil
}
