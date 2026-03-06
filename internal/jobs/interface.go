package jobs

import (
	"key_skew/internal/common"
)

// Job defines the interface for a MapReduce job
// This abstraction allows the framework to work with different job types
// without hardcoding job-specific logic in mapper, reducer, or master
type Job interface {
	// Name returns the job identifier (e.g., "wordcount", "pagerank")
	Name() string

	// Map processes an input record and emits key-value pairs
	Map(record common.InputRecord) []common.KV

	// Reduce aggregates values for a key and returns the result
	// The result type depends on the job (int for WordCount, string for PageRank)
	Reduce(key string, values interface{}) interface{}

	// SupportsSkewMitigation returns whether this job supports key salting
	// for skew mitigation (WordCount does, PageRank doesn't)
	SupportsSkewMitigation() bool

	// ValueType returns the type of values this job uses
	// "int" for WordCount, "string" for PageRank
	ValueType() string

	// OutputFormat returns how the reducer should write output
	// "kv" for standard KV records (WordCount), "raw" for raw JSON (PageRank)
	OutputFormat() string

	// GetParameters returns job-specific parameters as a map
	// Used for passing to reducers (e.g., damping, numNodes for PageRank)
	GetParameters() map[string]interface{}

	// SetParameters sets job-specific parameters
	SetParameters(params map[string]interface{}) error
}

// JobRegistry maintains a registry of available jobs
type JobRegistry struct {
	jobs map[string]Job
}

// NewJobRegistry creates a new job registry with default jobs registered
func NewJobRegistry() *JobRegistry {
	registry := &JobRegistry{
		jobs: make(map[string]Job),
	}

	// Register default jobs
	registry.Register(NewWordCountJob())
	registry.Register(NewPageRankJob())

	return registry
}

// Register adds a job to the registry
func (r *JobRegistry) Register(job Job) {
	r.jobs[job.Name()] = job
}

// Get retrieves a job by name
func (r *JobRegistry) Get(name string) (Job, bool) {
	job, ok := r.jobs[name]
	return job, ok
}

// List returns all registered job names
func (r *JobRegistry) List() []string {
	names := make([]string, 0, len(r.jobs))
	for name := range r.jobs {
		names = append(names, name)
	}
	return names
}
