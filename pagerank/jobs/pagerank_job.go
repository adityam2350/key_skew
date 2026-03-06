package jobs

import (
	"key_skew/common/common"
	commonjobs "key_skew/common/jobs"
)

func init() {
	commonjobs.RegisterJob(NewPageRankJob())
}

type PageRankJob struct {
	params map[string]interface{}
}

// NewPageRankJob creates a new PageRank job instance
func NewPageRankJob() commonjobs.Job {
	return &PageRankJob{
		params: make(map[string]interface{}),
	}
}

// Name returns the job name
func (j *PageRankJob) Name() string {
	return "pagerank"
}

// Map processes an input record and emits key-value pairs
func (j *PageRankJob) Map(record common.InputRecord) []common.KV {
	return MapRecordPageRank(record)
}

// Reduce aggregates values for a key and computes new rank
func (j *PageRankJob) Reduce(key string, values interface{}) interface{} {
	strValues, ok := values.([]string)
	if !ok {
		return "{}"
	}

	damping, ok := j.params["damping"].(float64)
	if !ok {
		damping = 0.85
	}

	numNodes, ok := j.params["num_nodes"].(int)
	if !ok {
		return `{"error":"num_nodes parameter required"}`
	}

	return ReduceKeyPageRank(key, strValues, damping, numNodes)
}

// SupportsSkewMitigation returns true if the job supports skew mitigation
func (j *PageRankJob) SupportsSkewMitigation() bool {
	return true
}

// ValueType returns the type of values
func (j *PageRankJob) ValueType() string {
	return "string"
}

// OutputFormat returns the output format
func (j *PageRankJob) OutputFormat() string {
	return "raw"
}

// GetParameters returns the current job parameters
func (j *PageRankJob) GetParameters() map[string]interface{} {
	return j.params
}

// SetParameters sets job parameters
func (j *PageRankJob) SetParameters(params map[string]interface{}) error {
	j.params = params
	return nil
}
