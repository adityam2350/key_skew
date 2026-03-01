package common

import (
	"fmt"
	"hash/crc32"
)

// PartitionKey computes a stable partition ID for a key using CRC32
// Key can be either:
// - Unsalted: string "word" -> []byte("word")
// - Salted: []interface{}{"word", salt} -> []byte("word#3")
// Returns: crc32(key_representation) % R
func PartitionKey(key interface{}, R int) (int, error) {
	var keyBytes []byte

	switch k := key.(type) {
	case string:
		keyBytes = []byte(k)
	case []interface{}:
		if len(k) == 2 {
			word, ok1 := k[0].(string)
			salt, ok2 := k[1].(float64)
			if ok1 && ok2 {
				keyBytes = []byte(fmt.Sprintf("%s#%.0f", word, salt))
			} else {
				// Try int for salt
				if word, ok1 := k[0].(string); ok1 {
					if saltInt, ok2 := k[1].(int); ok2 {
						keyBytes = []byte(fmt.Sprintf("%s#%d", word, saltInt))
					} else {
						return 0, fmt.Errorf("invalid salted key format: %v", key)
					}
				} else {
					return 0, fmt.Errorf("invalid salted key format: %v", key)
				}
			}
		} else {
			return 0, fmt.Errorf("invalid salted key format: expected 2 elements, got %d", len(k))
		}
	default:
		return 0, fmt.Errorf("unsupported key type for hashing: %T", key)
	}

	// Use IEEE polynomial (standard CRC32)
	hash := crc32.ChecksumIEEE(keyBytes)
	return int(hash) % R, nil
}
