package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"key_skew/common/common"
	commonjobs "key_skew/common/jobs"
	_ "key_skew/wordcount/jobs"
)

func TestRunExecuteModeSaltsHeavyKeys(t *testing.T) {
	tmpDir := t.TempDir()

	shardPath := filepath.Join(tmpDir, "shard.jsonl")
	const N = 40
	var sb strings.Builder
	for i := 0; i < N; i++ {
		sb.WriteString(`{"text":"basketball"}` + "\n")
	}
	if err := os.WriteFile(shardPath, []byte(sb.String()), 0644); err != nil {
		t.Fatalf("write shard: %v", err)
	}

	shardFile, err := os.Open(shardPath)
	if err != nil {
		t.Fatalf("open shard: %v", err)
	}
	defer shardFile.Close()

	outDir := filepath.Join(tmpDir, "out")
	if err := os.MkdirAll(outDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	job, ok := commonjobs.NewJobRegistry().Get("wordcount")
	if !ok {
		t.Fatal("wordcount job not registered")
	}

	const R = 4
	const Splits = 4
	plan := &common.PartitionPlan{
		R:     R,
		Heavy: map[string]common.HeavyKeyInfo{"basketball": {Splits: Splits}},
	}

	runExecuteMode(shardFile, outDir, job, R, plan, false)

	saltCounts := make(map[int]int)
	totalRecords := 0
	perFileCounts := make([]int, R)

	for r := 0; r < R; r++ {
		path := filepath.Join(outDir, fmt.Sprintf("part_%03d.jsonl", r))
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open part %d: %v", r, err)
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			var kv common.KV
			if err := json.Unmarshal(scanner.Bytes(), &kv); err != nil {
				t.Fatalf("decode kv: %v", err)
			}
			arr, ok := kv.K.([]interface{})
			if !ok {
				t.Fatalf("expected salted key []interface{}, got %T (%v)", kv.K, kv.K)
			}
			if len(arr) != 2 {
				t.Fatalf("expected salted key of length 2, got %v", arr)
			}
			base, ok := arr[0].(string)
			if !ok || base != "basketball" {
				t.Fatalf("expected base 'basketball', got %v", arr[0])
			}
			saltF, ok := arr[1].(float64)
			if !ok {
				t.Fatalf("expected salt float64, got %T", arr[1])
			}
			saltCounts[int(saltF)]++
			totalRecords++
			perFileCounts[r]++
		}
		f.Close()
	}

	if totalRecords != N {
		t.Errorf("expected %d total records, got %d", N, totalRecords)
	}

	expectedPerSalt := N / Splits
	for s := 0; s < Splits; s++ {
		if saltCounts[s] != expectedPerSalt {
			t.Errorf("salt %d: expected %d records, got %d", s, expectedPerSalt, saltCounts[s])
		}
	}
	if len(saltCounts) != Splits {
		t.Errorf("expected %d distinct salts, got %d (%v)", Splits, len(saltCounts), saltCounts)
	}

	t.Logf("per-file record counts: %v", perFileCounts)
	t.Logf("per-salt record counts: %v", saltCounts)
}

// TestRunExecuteModeCombiner verifies that with --combiner=true, 40 identical records for a
// heavy key with Splits=4 produce exactly 4 output records (one per salt value, count=10 each)
// instead of 40 raw records.
func TestRunExecuteModeCombiner(t *testing.T) {
	tmpDir := t.TempDir()

	shardPath := filepath.Join(tmpDir, "shard.jsonl")
	const N = 40
	var sb strings.Builder
	for i := 0; i < N; i++ {
		sb.WriteString(`{"text":"basketball"}` + "\n")
	}
	if err := os.WriteFile(shardPath, []byte(sb.String()), 0644); err != nil {
		t.Fatalf("write shard: %v", err)
	}

	shardFile, err := os.Open(shardPath)
	if err != nil {
		t.Fatalf("open shard: %v", err)
	}
	defer shardFile.Close()

	outDir := filepath.Join(tmpDir, "out")
	if err := os.MkdirAll(outDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	job, ok := commonjobs.NewJobRegistry().Get("wordcount")
	if !ok {
		t.Fatal("wordcount job not registered")
	}

	const R = 4
	const Splits = 4
	plan := &common.PartitionPlan{
		R:     R,
		Heavy: map[string]common.HeavyKeyInfo{"basketball": {Splits: Splits}},
	}

	runExecuteMode(shardFile, outDir, job, R, plan, true /* combiner */)

	saltCounts := make(map[int]int)   // salt value → combined count stored in V
	totalRecords := 0

	for r := 0; r < R; r++ {
		path := filepath.Join(outDir, fmt.Sprintf("part_%03d.jsonl", r))
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open part %d: %v", r, err)
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			var kv common.KV
			if err := json.Unmarshal(scanner.Bytes(), &kv); err != nil {
				t.Fatalf("decode kv: %v", err)
			}
			arr, ok := kv.K.([]interface{})
			if !ok {
				t.Fatalf("expected salted key []interface{}, got %T (%v)", kv.K, kv.K)
			}
			saltF, ok := arr[1].(float64)
			if !ok {
				t.Fatalf("expected salt float64, got %T", arr[1])
			}
			count, ok := kv.V.(float64)
			if !ok {
				t.Fatalf("expected combined count float64, got %T (%v)", kv.V, kv.V)
			}
			saltCounts[int(saltF)] += int(count)
			totalRecords++
		}
		f.Close()
	}

	// Combiner must emit exactly one record per salt value (4 records total), not 40.
	if totalRecords != Splits {
		t.Errorf("expected %d combined records (one per salt), got %d", Splits, totalRecords)
	}

	expectedCountPerSalt := N / Splits
	for s := 0; s < Splits; s++ {
		if saltCounts[s] != expectedCountPerSalt {
			t.Errorf("salt %d: expected combined count %d, got %d", s, expectedCountPerSalt, saltCounts[s])
		}
	}

	t.Logf("combined records per salt: %v", saltCounts)
}
