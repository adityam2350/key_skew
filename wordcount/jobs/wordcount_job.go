package jobs

import (
	"key_skew/common/common"
	commonjobs "key_skew/common/jobs"
)

func init() {
	commonjobs.RegisterJob(NewWordCountJob())
}

type WordCountJob struct {
	params map[string]interface{}
}

// NewWordCountJob creates a new WordCount job instance
func NewWordCountJob() commonjobs.Job {
	return &WordCountJob{
		params: make(map[string]interface{}),
	}
}

// Name returns the job name
func (j *WordCountJob) Name() string {
	return "wordcount"
}

// Map processes an input record and emits key-value pairs
func (j *WordCountJob) Map(record common.InputRecord) []common.KV {
	return MapRecord(record)
}

// Reduce aggregates values for a key
func (j *WordCountJob) Reduce(key string, values interface{}) interface{} {
	intValues, ok := values.([]int)
	if !ok {
		return 0
	}
	return ReduceKey(key, intValues)
}

// SupportsSkewMitigation returns true if the job supports skew mitigation
func (j *WordCountJob) SupportsSkewMitigation() bool {
	return true
}

// ValueType returns the type of values
func (j *WordCountJob) ValueType() string {
	return "int"
}

// OutputFormat returns the output format
func (j *WordCountJob) OutputFormat() string {
	return "kv"
}

// GetParameters returns the current job parameters
func (j *WordCountJob) GetParameters() map[string]interface{} {
	return j.params
}

// SetParameters sets job parameters
func (j *WordCountJob) SetParameters(params map[string]interface{}) error {
	j.params = params
	return nil
}
