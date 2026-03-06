package heavy

import (
	"encoding/json"
	"fmt"
	"os"
)

type HeavyConfig struct {
	HeavyNodes map[string]int `json:"heavy_nodes"`
}

// LoadHeavyConfig loads a heavy-hitter configuration from a JSON file
func LoadHeavyConfig(path string) (*HeavyConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open heavy config file: %w", err)
	}
	defer file.Close()

	var config HeavyConfig
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to decode heavy config: %w", err)
	}

	if config.HeavyNodes == nil {
		config.HeavyNodes = make(map[string]int)
	}

	return &config, nil
}

// IsHeavy checks if a node is configured as a heavy destination
func (c *HeavyConfig) IsHeavy(nodeID string) bool {
	if c == nil || c.HeavyNodes == nil {
		return false
	}
	_, exists := c.HeavyNodes[nodeID]
	return exists
}

// SplitCount returns the number of splits for a heavy node
func (c *HeavyConfig) SplitCount(nodeID string) int {
	if c == nil || c.HeavyNodes == nil {
		return 0
	}
	return c.HeavyNodes[nodeID]
}

// Save writes the heavy config to a JSON file
func (c *HeavyConfig) Save(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create heavy config file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(c); err != nil {
		return fmt.Errorf("failed to encode heavy config: %w", err)
	}

	return nil
}
