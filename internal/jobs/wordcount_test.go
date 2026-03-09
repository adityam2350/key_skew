package jobs

import (
	"testing"

	"key_skew/internal/common"
)

// --- Tokenize ---

func TestTokenize_Empty(t *testing.T) {
	tokens := Tokenize("")
	if len(tokens) != 0 {
		t.Errorf("expected 0 tokens, got %d", len(tokens))
	}
}

func TestTokenize_SingleWord(t *testing.T) {
	tokens := Tokenize("hello")
	if len(tokens) != 1 || tokens[0] != "hello" {
		t.Errorf("expected [hello], got %v", tokens)
	}
}

func TestTokenize_MultipleWords(t *testing.T) {
	tokens := Tokenize("the quick brown fox")
	expected := []string{"the", "quick", "brown", "fox"}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}
	for i, tok := range tokens {
		if tok != expected[i] {
			t.Errorf("token[%d]: expected %q, got %q", i, expected[i], tok)
		}
	}
}

func TestTokenize_LowercasesInput(t *testing.T) {
	tokens := Tokenize("Hello WORLD")
	if tokens[0] != "hello" || tokens[1] != "world" {
		t.Errorf("expected lowercase tokens, got %v", tokens)
	}
}

func TestTokenize_ExtraWhitespace(t *testing.T) {
	tokens := Tokenize("  foo   bar  ")
	if len(tokens) != 2 || tokens[0] != "foo" || tokens[1] != "bar" {
		t.Errorf("expected [foo bar], got %v", tokens)
	}
}

func TestTokenize_OnlyWhitespace(t *testing.T) {
	tokens := Tokenize("   \t  ")
	if len(tokens) != 0 {
		t.Errorf("expected 0 tokens, got %d: %v", len(tokens), tokens)
	}
}

// --- MapRecord ---

func TestMapRecord_EmitsOneKVPerToken(t *testing.T) {
	record := common.InputRecord{Text: "cat sat mat"}
	kvs := MapRecord(record)
	if len(kvs) != 3 {
		t.Fatalf("expected 3 KVs, got %d", len(kvs))
	}
	for i, kv := range kvs {
		if kv.V != 1 {
			t.Errorf("kv[%d].V: expected 1, got %d", i, kv.V)
		}
	}
}

func TestMapRecord_KeysMatchTokens(t *testing.T) {
	record := common.InputRecord{Text: "Hello World"}
	kvs := MapRecord(record)
	// keys should be lowercase tokens
	keys := []string{}
	for _, kv := range kvs {
		keys = append(keys, kv.K.(string))
	}
	if keys[0] != "hello" || keys[1] != "world" {
		t.Errorf("expected [hello world], got %v", keys)
	}
}

func TestMapRecord_EmptyText(t *testing.T) {
	record := common.InputRecord{Text: ""}
	kvs := MapRecord(record)
	if len(kvs) != 0 {
		t.Errorf("expected 0 KVs for empty text, got %d", len(kvs))
	}
}

func TestMapRecord_RepeatedWord(t *testing.T) {
	record := common.InputRecord{Text: "the the the"}
	kvs := MapRecord(record)
	// Mapper emits one KV per occurrence (not deduplicated)
	if len(kvs) != 3 {
		t.Errorf("expected 3 KVs for 3 occurrences of 'the', got %d", len(kvs))
	}
	for _, kv := range kvs {
		if kv.K.(string) != "the" {
			t.Errorf("expected key 'the', got %v", kv.K)
		}
	}
}

// --- ReduceKey ---

func TestReduceKey_SumsValues(t *testing.T) {
	result := ReduceKey("the", []int{3, 5, 2})
	if result != 10 {
		t.Errorf("expected 10, got %d", result)
	}
}

func TestReduceKey_SingleValue(t *testing.T) {
	result := ReduceKey("word", []int{7})
	if result != 7 {
		t.Errorf("expected 7, got %d", result)
	}
}

func TestReduceKey_EmptyValues(t *testing.T) {
	result := ReduceKey("word", []int{})
	if result != 0 {
		t.Errorf("expected 0 for empty values, got %d", result)
	}
}

func TestReduceKey_AllOnes(t *testing.T) {
	// Typical MapReduce word count: N mappers each emit (word, 1)
	values := make([]int, 100)
	for i := range values {
		values[i] = 1
	}
	result := ReduceKey("the", values)
	if result != 100 {
		t.Errorf("expected 100, got %d", result)
	}
}
