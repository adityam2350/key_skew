package common

type InputRecord struct {
	Text string `json:"text"`
}

type KV struct {
	K interface{} `json:"k"`
	V interface{} `json:"v"`
}

type OutputRecord struct {
	K interface{} `json:"k"`
	V interface{} `json:"v"`
}

type ReducerLoadMetrics struct {
	Bytes   []int64 `json:"bytes"`
	Records []int64 `json:"records"`
}

type PartitionPlan struct {
	R     int                     `json:"R"`
	Heavy map[string]HeavyKeyInfo `json:"heavy"`
}

type HeavyKeyInfo struct {
	Splits int `json:"splits"`
}
