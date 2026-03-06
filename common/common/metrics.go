package common

import (
	"math"
	"sort"
)

// ComputeCoV computes the coefficient of variation (std/mean) for a slice of values
// Returns 0 if mean is 0
func ComputeCoV(values []int64) float64 {
	if len(values) == 0 {
		return 0.0
	}

	mean := computeMean(values)
	if mean == 0 {
		return 0.0
	}

	std := computeStd(values, mean)
	return std / mean
}

// ComputeMaxMedianRatio computes the ratio of max to median value
// Returns 0 if median is 0
func ComputeMaxMedianRatio(values []int64) float64 {
	if len(values) == 0 {
		return 0.0
	}

	sorted := make([]int64, len(values))
	copy(sorted, values)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	median := computeMedian(sorted)
	if median == 0 {
		return 0.0
	}

	max := sorted[len(sorted)-1]
	return float64(max) / float64(median)
}

// computeMean computes the arithmetic mean
func computeMean(values []int64) float64 {
	if len(values) == 0 {
		return 0.0
	}

	var sum int64
	for _, v := range values {
		sum += v
	}
	return float64(sum) / float64(len(values))
}

// computeStd computes the standard deviation
func computeStd(values []int64, mean float64) float64 {
	if len(values) == 0 {
		return 0.0
	}

	var sumSqDiff float64
	for _, v := range values {
		diff := float64(v) - mean
		sumSqDiff += diff * diff
	}

	variance := sumSqDiff / float64(len(values))
	return math.Sqrt(variance)
}

// computeMedian computes the median value
func computeMedian(sorted []int64) int64 {
	if len(sorted) == 0 {
		return 0
	}

	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2
	}
	return sorted[mid]
}

// ReducerLoadMetrics aggregates reducer load statistics
type ReducerLoadMetrics struct {
	Bytes   []int64 `json:"bytes"`
	Records []int64 `json:"records"`
}

// ComputeReducerLoadMetrics computes load distribution metrics from reducer stats
func ComputeReducerLoadMetrics(loads ReducerLoadMetrics) (CoVBytes, CoVRecords, MaxMedBytes, MaxMedRecords float64) {
	CoVBytes = ComputeCoV(loads.Bytes)
	CoVRecords = ComputeCoV(loads.Records)
	MaxMedBytes = ComputeMaxMedianRatio(loads.Bytes)
	MaxMedRecords = ComputeMaxMedianRatio(loads.Records)
	return
}
