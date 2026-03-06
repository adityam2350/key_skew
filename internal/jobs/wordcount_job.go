package jobs

import (
	"key_skew/internal/common"
)

// WordCountJob implements the Job interface for word counting
type WordCountJob struct {
	params map[string]interface{}
}

// NewWordCountJob creates a new WordCount job instance
func NewWordCountJob() Job {
	return &WordCountJob{
		params: make(map[string]interface{}),
	}
}

func (j *WordCountJob) Name() string {
	return "wordcount"
}

func (j *WordCountJob) Map(record common.InputRecord) []common.KV {
	return MapRecord(record)
}

func (j *WordCountJob) Reduce(key string, values interface{}) interface{} {
	intValues, ok := values.([]int)
	if !ok {
		// Try to convert if possible
		return 0
	}
	return ReduceKey(key, intValues)
}

func (j *WordCountJob) SupportsSkewMitigation() bool {
	return true
}

func (j *WordCountJob) ValueType() string {
	return "int"
}

func (j *WordCountJob) OutputFormat() string {
	return "kv"
}

func (j *WordCountJob) GetParameters() map[string]interface{} {
	return j.params
}

func (j *WordCountJob) SetParameters(params map[string]interface{}) error {
	// WordCount has no special parameters
	j.params = params
	return nil
}
