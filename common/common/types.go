package common

import (
	"encoding/json"
	"fmt"
)

// InputRecord represents a single input record for MapReduce jobs
type InputRecord struct {
	Text string `json:"text"`
}

// KV represents a key-value pair in intermediate and output records
// V can be int (for WordCount) or string (for PageRank JSON values)
type KV struct {
	K interface{} `json:"k"`
	V interface{} `json:"v"`
}

// OutputRecord is an alias for KV, used for reducer outputs
type OutputRecord = KV

// HeavyKeyInfo contains information about a heavy hitter key
type HeavyKeyInfo struct {
	Splits int `json:"splits"`
}

// PartitionPlan defines the partitioning strategy, including heavy hitters
type PartitionPlan struct {
	R     int                     `json:"R"`
	Heavy map[string]HeavyKeyInfo `json:"heavy"`
}

// CanonicalKey converts a key (string or [string, int] array) to a canonical string representation
// Used for grouping keys in reducers
// - Unsalted key "word" -> "word"
// - Salted key ["word", 3] -> "word#3"
func CanonicalKey(key interface{}) (string, error) {
	switch k := key.(type) {
	case string:
		return k, nil
	case []interface{}:
		if len(k) == 2 {
			word, ok1 := k[0].(string)
			salt, ok2 := k[1].(float64)
			if ok1 && ok2 {
				return fmt.Sprintf("%s#%.0f", word, salt), nil
			}
		}
		return "", fmt.Errorf("invalid salted key format: %v", key)
	default:
		return "", fmt.Errorf("unsupported key type: %T", key)
	}
}

// ExtractBaseKey extracts the base key from a salted or unsalted key
// Returns the original word without the salt
func ExtractBaseKey(key interface{}) (string, error) {
	switch k := key.(type) {
	case string:
		return k, nil
	case []interface{}:
		if len(k) == 2 {
			word, ok := k[0].(string)
			if ok {
				return word, nil
			}
		}
		return "", fmt.Errorf("invalid salted key format: %v", key)
	default:
		return "", fmt.Errorf("unsupported key type: %T", key)
	}
}

// IsSaltedKey checks if a key is salted (array format)
func IsSaltedKey(key interface{}) bool {
	_, ok := key.([]interface{})
	return ok
}

// CreateSaltedKey creates a salted key from a word and salt value
func CreateSaltedKey(word string, salt int) interface{} {
	return []interface{}{word, salt}
}

// UnmarshalKey unmarshals a JSON key value into the appropriate Go type
func UnmarshalKey(keyJSON json.RawMessage) (interface{}, error) {
	// Try as string first
	var strKey string
	if err := json.Unmarshal(keyJSON, &strKey); err == nil {
		return strKey, nil
	}

	// Try as array
	var arrKey []interface{}
	if err := json.Unmarshal(keyJSON, &arrKey); err == nil {
		return arrKey, nil
	}

	return nil, fmt.Errorf("cannot unmarshal key: %s", string(keyJSON))
}
