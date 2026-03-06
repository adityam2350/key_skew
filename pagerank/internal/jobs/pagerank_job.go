package jobs

import (
	"fmt"

	"key_skew/common/common"
	commonjobs "key_skew/common/jobs"
)

func init() {
	// Register PageRank job in the global registry
	commonjobs.RegisterJob(NewPageRankJob())
}

// PageRankJob implements the Job interface for PageRank computation
type PageRankJob struct {
	params map[string]interface{}
}

// NewPageRankJob creates a new PageRank job instance
func NewPageRankJob() commonjobs.Job {
	return &PageRankJob{
		params: make(map[string]interface{}),
	}
}

func (j *PageRankJob) Name() string {
	return "pagerank"
}

func (j *PageRankJob) Map(record common.InputRecord) []common.KV {
	return MapRecordPageRank(record)
}

func (j *PageRankJob) Reduce(key string, values interface{}) interface{} {
	strValues, ok := values.([]string)
	if !ok {
		return "{}"
	}

	// Extract parameters
	damping, ok := j.params["damping"].(float64)
	if !ok {
		damping = 0.85 // Default
	}

	numNodes, ok := j.params["num_nodes"].(int)
	if !ok {
		return `{"error":"num_nodes parameter required"}`
	}

	return ReduceKeyPageRank(key, strValues, damping, numNodes)
}

func (j *PageRankJob) SupportsSkewMitigation() bool {
	return false
}

func (j *PageRankJob) ValueType() string {
	return "string"
}

func (j *PageRankJob) OutputFormat() string {
	return "raw"
}

func (j *PageRankJob) GetParameters() map[string]interface{} {
	return j.params
}

func (j *PageRankJob) SetParameters(params map[string]interface{}) error {
	j.params = params

	// Validate required parameters
	if _, ok := params["num_nodes"]; !ok {
		return fmt.Errorf("num_nodes parameter is required for PageRank")
	}

	return nil
}
