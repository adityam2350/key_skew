package jobs

import (
	"strings"

	"key_skew/internal/common"
)

// MapRecord processes an input record and emits key-value pairs
// For WordCount: tokenizes text and emits (word, 1) for each token
func MapRecord(record common.InputRecord) []common.KV {
	tokens := Tokenize(record.Text)
	kvs := make([]common.KV, 0, len(tokens))

	for _, token := range tokens {
		kvs = append(kvs, common.KV{
			K: token,
			V: 1,
		})
	}

	return kvs
}

// ReduceKey aggregates values for a key
// For WordCount: sums all values
func ReduceKey(key string, values []int) int {
	sum := 0
	for _, v := range values {
		sum += v
	}
	return sum
}

// Tokenize splits text into tokens (words)
// Splits on whitespace, converts to lowercase, and filters empty strings
func Tokenize(text string) []string {
	words := strings.Fields(strings.ToLower(text))
	tokens := make([]string, 0, len(words))

	for _, word := range words {
		trimmed := strings.TrimSpace(word)
		if len(trimmed) > 0 {
			tokens = append(tokens, trimmed)
		}
	}

	return tokens
}
