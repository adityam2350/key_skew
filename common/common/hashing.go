package common

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
)

// PartitionKey computes which reducer should handle a key
func PartitionKey(key interface{}, R int) (int, error) {
	canonical, err := CanonicalKey(key)
	if err != nil {
		return 0, err
	}
	return int(crc32.ChecksumIEEE([]byte(canonical))) % R, nil
}

// CanonicalKey converts a key to a canonical string representation
func CanonicalKey(key interface{}) (string, error) {
	switch k := key.(type) {
	case string:
		return k, nil
	case []interface{}:
		if len(k) != 2 {
			return "", fmt.Errorf("invalid salted key format: expected [key, salt], got %v", k)
		}
		baseKey, ok := k[0].(string)
		if !ok {
			return "", fmt.Errorf("invalid salted key: base key must be string, got %T", k[0])
		}
		salt, ok := k[1].(float64)
		if !ok {
			return "", fmt.Errorf("invalid salted key: salt must be number, got %T", k[1])
		}
		return fmt.Sprintf("%s#%.0f", baseKey, salt), nil
	default:
		jsonBytes, err := json.Marshal(key)
		if err != nil {
			return "", fmt.Errorf("failed to canonicalize key %v: %w", key, err)
		}
		return string(jsonBytes), nil
	}
}

// CreateSaltedKey creates a salted key representation
func CreateSaltedKey(baseKey string, salt int) interface{} {
	return []interface{}{baseKey, salt}
}

// ExtractBaseKey extracts the base key from a potentially salted key
func ExtractBaseKey(key interface{}) (string, error) {
	switch k := key.(type) {
	case string:
		return k, nil
	case []interface{}:
		if len(k) != 2 {
			return "", fmt.Errorf("invalid salted key format: expected [key, salt], got %v", k)
		}
		baseKey, ok := k[0].(string)
		if !ok {
			return "", fmt.Errorf("invalid salted key: base key must be string, got %T", k[0])
		}
		return baseKey, nil
	default:
		return fmt.Sprintf("%v", key), nil
	}
}
