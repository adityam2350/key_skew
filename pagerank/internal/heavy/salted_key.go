package heavy

import (
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
)

type SaltedKey struct {
	NodeID   string `json:"node_id"`
	Salt     int    `json:"salt"`
	IsSalted bool   `json:"is_salted"`
}

// CreateSaltedKey creates a salted key for a heavy node
func CreateSaltedKey(nodeID string, salt int) SaltedKey {
	return SaltedKey{
		NodeID:   nodeID,
		Salt:     salt,
		IsSalted: true,
	}
}

// CreateUnsaltedKey creates an unsalted key for a normal node
func CreateUnsaltedKey(nodeID string) SaltedKey {
	return SaltedKey{
		NodeID:   nodeID,
		Salt:     0,
		IsSalted: false,
	}
}

// EncodeKey encodes a SaltedKey to a string for use as a MapReduce key
func EncodeKey(key SaltedKey) string {
	if !key.IsSalted {
		return key.NodeID
	}
	return fmt.Sprintf("%s#%d", key.NodeID, key.Salt)
}

// DecodeKey parses an encoded key string back into a SaltedKey
func DecodeKey(encoded string) (SaltedKey, error) {
	parts := strings.Split(encoded, "#")
	if len(parts) == 1 {
		return CreateUnsaltedKey(parts[0]), nil
	} else if len(parts) == 2 {
		salt, err := strconv.Atoi(parts[1])
		if err != nil {
			return SaltedKey{}, fmt.Errorf("invalid salt in key: %w", err)
		}
		return CreateSaltedKey(parts[0], salt), nil
	}
	return SaltedKey{}, fmt.Errorf("invalid key format: %s", encoded)
}

// GetOriginalNodeID extracts the original node ID from a salted or unsalted key
func GetOriginalNodeID(key SaltedKey) string {
	return key.NodeID
}

// ComputeSalt computes a deterministic salt index for a source-destination pair
func ComputeSalt(sourceNodeID, destNodeID string, splitCount int) int {
	if splitCount <= 0 {
		return 0
	}
	hash := crc32.ChecksumIEEE([]byte(sourceNodeID))
	return int(hash) % splitCount
}
