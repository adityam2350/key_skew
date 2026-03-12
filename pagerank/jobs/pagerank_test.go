package jobs

import (
	"encoding/json"
	"testing"

	"key_skew/common/common"
)

// --- MapRecordPageRank tests ---

func TestMapRecordPageRank_InvalidJSON(t *testing.T) {
	record := common.InputRecord{Text: "not json"}
	kvs := MapRecordPageRank(record)
	if len(kvs) != 0 {
		t.Errorf("expected 0 kvs for invalid JSON, got %d", len(kvs))
	}
}

func TestMapRecordPageRank_NoNeighbors(t *testing.T) {
	node := PageRankNodeRecord{Node: "A", Rank: 1.0, Neighbors: []string{}}
	data, _ := json.Marshal(node)
	kvs := MapRecordPageRank(common.InputRecord{Text: string(data)})

	// Should emit exactly 1 KV: the adjacency entry for "A"
	if len(kvs) != 1 {
		t.Fatalf("expected 1 kv (adjacency only), got %d", len(kvs))
	}
	if kvs[0].K != "A" {
		t.Errorf("expected key 'A', got %v", kvs[0].K)
	}
	var prv PageRankValue
	if err := json.Unmarshal([]byte(kvs[0].V.(string)), &prv); err != nil {
		t.Fatalf("failed to parse value: %v", err)
	}
	if prv.Type != "adjacency" {
		t.Errorf("expected type 'adjacency', got %q", prv.Type)
	}
}

func TestMapRecordPageRank_WithNeighbors(t *testing.T) {
	node := PageRankNodeRecord{Node: "A", Rank: 1.0, Neighbors: []string{"B", "C"}}
	data, _ := json.Marshal(node)
	kvs := MapRecordPageRank(common.InputRecord{Text: string(data)})

	// 1 adjacency + 2 contributions = 3 kvs
	if len(kvs) != 3 {
		t.Fatalf("expected 3 kvs, got %d", len(kvs))
	}

	// First kv: adjacency for self
	if kvs[0].K != "A" {
		t.Errorf("expected first key 'A', got %v", kvs[0].K)
	}
	var adj PageRankValue
	json.Unmarshal([]byte(kvs[0].V.(string)), &adj)
	if adj.Type != "adjacency" {
		t.Errorf("expected adjacency type, got %q", adj.Type)
	}

	// Remaining: contribution kvs for B and C
	neighborKeys := map[string]bool{"B": false, "C": false}
	expectedContrib := 1.0 / 2.0
	for _, kv := range kvs[1:] {
		key := kv.K.(string)
		if _, ok := neighborKeys[key]; !ok {
			t.Errorf("unexpected contribution key %q", key)
		}
		neighborKeys[key] = true
		var cv PageRankValue
		json.Unmarshal([]byte(kv.V.(string)), &cv)
		if cv.Type != "contribution" {
			t.Errorf("expected contribution type, got %q", cv.Type)
		}
		if cv.Amount != expectedContrib {
			t.Errorf("expected contribution %.4f, got %.4f", expectedContrib, cv.Amount)
		}
	}
	for k, seen := range neighborKeys {
		if !seen {
			t.Errorf("missing contribution kv for neighbor %q", k)
		}
	}
}

func TestMapRecordPageRank_ContributionSplitEvenly(t *testing.T) {
	node := PageRankNodeRecord{Node: "X", Rank: 3.0, Neighbors: []string{"A", "B", "C"}}
	data, _ := json.Marshal(node)
	kvs := MapRecordPageRank(common.InputRecord{Text: string(data)})

	expected := 3.0 / 3.0
	for _, kv := range kvs[1:] {
		var cv PageRankValue
		json.Unmarshal([]byte(kv.V.(string)), &cv)
		if cv.Amount != expected {
			t.Errorf("expected contribution 1.0, got %f", cv.Amount)
		}
	}
}

// --- ReduceKeyPageRank tests ---

func makeContrib(amount float64) string {
	v := PageRankValue{Type: "contribution", Amount: amount}
	b, _ := json.Marshal(v)
	return string(b)
}

func makeAdj(neighbors []string) string {
	v := PageRankValue{Type: "adjacency", Neighbors: neighbors}
	b, _ := json.Marshal(v)
	return string(b)
}

func TestReduceKeyPageRank_BasicRankComputation(t *testing.T) {
	damping := 0.85
	numNodes := 4
	values := []string{
		makeAdj([]string{"B", "C"}),
		makeContrib(0.5),
		makeContrib(0.3),
	}
	result := ReduceKeyPageRank("A", values, damping, numNodes)

	var rec PageRankNodeRecord
	if err := json.Unmarshal([]byte(result), &rec); err != nil {
		t.Fatalf("failed to parse result: %v", err)
	}

	expectedRank := (1.0-damping)/float64(numNodes) + damping*(0.5+0.3)
	if abs(rec.Rank-expectedRank) > 1e-9 {
		t.Errorf("expected rank %.6f, got %.6f", expectedRank, rec.Rank)
	}
	if rec.Node != "A" {
		t.Errorf("expected node 'A', got %q", rec.Node)
	}
	if len(rec.Neighbors) != 2 {
		t.Errorf("expected 2 neighbors, got %d", len(rec.Neighbors))
	}
}

func TestReduceKeyPageRank_NoContributions(t *testing.T) {
	damping := 0.85
	numNodes := 10
	values := []string{makeAdj([]string{"B"})}
	result := ReduceKeyPageRank("A", values, damping, numNodes)

	var rec PageRankNodeRecord
	json.Unmarshal([]byte(result), &rec)

	expectedRank := (1.0 - damping) / float64(numNodes)
	if abs(rec.Rank-expectedRank) > 1e-9 {
		t.Errorf("expected rank %.6f, got %.6f", expectedRank, rec.Rank)
	}
}

func TestReduceKeyPageRank_SaltedKeyEmitsPartial(t *testing.T) {
	values := []string{makeContrib(0.4), makeContrib(0.6)}
	result := ReduceKeyPageRank("A#2", values, 0.85, 10)

	var rec PageRankNodeRecord
	if err := json.Unmarshal([]byte(result), &rec); err != nil {
		t.Fatalf("failed to parse salted result: %v", err)
	}

	// Salted output should carry the salted key and summed contribution, no final rank
	if rec.Node != "A#2" {
		t.Errorf("expected node 'A#2', got %q", rec.Node)
	}
	if abs(rec.Rank-1.0) > 1e-9 {
		t.Errorf("expected partial rank sum 1.0, got %f", rec.Rank)
	}
	if len(rec.Neighbors) != 0 {
		t.Errorf("salted output should have no neighbors, got %v", rec.Neighbors)
	}
}

func TestReduceKeyPageRank_SaltedKeyIgnoresAdjacency(t *testing.T) {
	// Adjacency values for salted keys should be ignored
	values := []string{makeAdj([]string{"X", "Y"}), makeContrib(0.5)}
	result := ReduceKeyPageRank("A#1", values, 0.85, 10)

	var rec PageRankNodeRecord
	json.Unmarshal([]byte(result), &rec)

	if len(rec.Neighbors) != 0 {
		t.Errorf("salted key should ignore adjacency, got neighbors %v", rec.Neighbors)
	}
}

func TestReduceKeyPageRank_AdjacencyPreserved(t *testing.T) {
	neighbors := []string{"B", "C", "D"}
	values := []string{makeAdj(neighbors), makeContrib(0.2)}
	result := ReduceKeyPageRank("A", values, 0.85, 5)

	var rec PageRankNodeRecord
	json.Unmarshal([]byte(result), &rec)

	if len(rec.Neighbors) != len(neighbors) {
		t.Errorf("expected %d neighbors, got %d", len(neighbors), len(rec.Neighbors))
	}
}

// --- ParsePageRankNodeRecord tests ---

func TestParsePageRankNodeRecord_Valid(t *testing.T) {
	input := `{"node":"A","rank":0.25,"neighbors":["B","C"]}`
	rec, err := ParsePageRankNodeRecord(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Node != "A" || rec.Rank != 0.25 || len(rec.Neighbors) != 2 {
		t.Errorf("unexpected record: %+v", rec)
	}
}

func TestParsePageRankNodeRecord_Invalid(t *testing.T) {
	_, err := ParsePageRankNodeRecord("not json")
	if err == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
