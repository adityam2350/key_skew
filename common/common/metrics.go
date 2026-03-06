package common

import (
	"math"
)

// ComputeReducerLoadMetrics computes coefficient of variation and max/median ratio
func ComputeReducerLoadMetrics(load ReducerLoadMetrics) (covBytes, covRecords, maxMedBytes, maxMedRecords float64) {
	covBytes = computeCoV(load.Bytes)
	covRecords = computeCoV(load.Records)
	maxMedBytes = computeMaxMedianRatio(load.Bytes)
	maxMedRecords = computeMaxMedianRatio(load.Records)
	return
}

// computeCoV computes the coefficient of variation
func computeCoV(values []int64) float64 {
	if len(values) == 0 {
		return 0.0
	}

	var sum int64
	for _, v := range values {
		sum += v
	}
	mean := float64(sum) / float64(len(values))

	if mean == 0 {
		return 0.0
	}

	var variance float64
	for _, v := range values {
		diff := float64(v) - mean
		variance += diff * diff
	}
	variance /= float64(len(values))

	stdDev := math.Sqrt(variance)
	return stdDev / mean
}

// computeMaxMedianRatio computes max divided by median ratio
func computeMaxMedianRatio(values []int64) float64 {
	if len(values) == 0 {
		return 0.0
	}

	sorted := make([]int64, len(values))
	copy(sorted, values)

	for i := 0; i < len(sorted)-1; i++ {
		for j := 0; j < len(sorted)-i-1; j++ {
			if sorted[j] > sorted[j+1] {
				sorted[j], sorted[j+1] = sorted[j+1], sorted[j]
			}
		}
	}

	median := sorted[len(sorted)/2]
	if len(sorted)%2 == 0 && len(sorted) > 1 {
		median = (sorted[len(sorted)/2-1] + sorted[len(sorted)/2]) / 2
	}

	if median == 0 {
		return 0.0
	}

	max := sorted[len(sorted)-1]
	return float64(max) / float64(median)
}
