package jobs

import (
	"key_skew/common/common"
)

type Job interface {
	Name() string
	Map(record common.InputRecord) []common.KV
	Reduce(key string, values interface{}) interface{}
	SupportsSkewMitigation() bool
	ValueType() string
	OutputFormat() string
	GetParameters() map[string]interface{}
	SetParameters(params map[string]interface{}) error
}

type JobRegistry struct {
	jobs map[string]Job
}

var globalRegistry = &JobRegistry{
	jobs: make(map[string]Job),
}

// NewJobRegistry creates a new job registry
func NewJobRegistry() *JobRegistry {
	return globalRegistry
}

// RegisterJob registers a job in the global registry
func RegisterJob(job Job) {
	globalRegistry.jobs[job.Name()] = job
}

// Get retrieves a job by name
func (r *JobRegistry) Get(name string) (Job, bool) {
	job, ok := r.jobs[name]
	return job, ok
}
