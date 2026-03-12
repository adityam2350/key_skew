package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"key_skew/common/common"
	commonjobs "key_skew/common/jobs"
	_ "key_skew/pagerank/jobs"
	_ "key_skew/wordcount/jobs"
)

func main() {
	var reduceID int
	var inputsPath string
	var outPath string
	var jobName string
	var damping float64
	var numNodes int

	flag.IntVar(&reduceID, "reduce-id", 0, "Reducer ID")
	flag.StringVar(&inputsPath, "inputs", "", "File containing list of input file paths")
	flag.StringVar(&outPath, "out", "", "Output file path")
	flag.StringVar(&jobName, "job", "", "Job name (wordcount or pagerank)")
	flag.Float64Var(&damping, "damping", 0.85, "PageRank damping factor (pagerank only)")
	flag.IntVar(&numNodes, "num-nodes", 0, "Number of nodes (pagerank only)")
	flag.Parse()

	if inputsPath == "" || outPath == "" || jobName == "" {
		fmt.Fprintf(os.Stderr, "Error: --inputs, --out, and --job are required\n")
		os.Exit(1)
	}

	registry := commonjobs.NewJobRegistry()
	job, ok := registry.Get(jobName)
	if !ok {
		fmt.Fprintf(os.Stderr, "Error: Unknown job: %s\n", jobName)
		os.Exit(1)
	}

	params := make(map[string]interface{})
	if jobName == "pagerank" {
		params["damping"] = damping
		params["num_nodes"] = numNodes
	}
	if err := job.SetParameters(params); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to set job parameters: %v\n", err)
		os.Exit(1)
	}

	inputFiles, err := common.ReadReducerInputs(inputsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to read inputs: %v\n", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create output directory: %v\n", err)
		os.Exit(1)
	}

	keyValues := make(map[string][]interface{})
	var recordsRead int64
	var bytesRead int64

	for _, inputFile := range inputFiles {
		file, err := os.Open(inputFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Failed to open %s: %v\n", inputFile, err)
			continue
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Bytes()
			bytesRead += int64(len(line))
			if len(line) == 0 {
				continue
			}

			var kv common.KV
			if err := json.Unmarshal(line, &kv); err != nil {
				continue
			}

			recordsRead++

			keyStr := fmt.Sprintf("%v", kv.K)
			keyValues[keyStr] = append(keyValues[keyStr], kv.V)
		}

		file.Close()
		if err := scanner.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Error reading %s: %v\n", inputFile, err)
		}
	}

	outFile, err := os.Create(outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create output file: %v\n", err)
		os.Exit(1)
	}
	defer outFile.Close()

	encoder := json.NewEncoder(outFile)
	var recordsWritten int64

	for keyStr, values := range keyValues {
		var reduced interface{}
		if job.ValueType() == "int" {
			intValues := make([]int, len(values))
			for i, v := range values {
				if intVal, ok := v.(int); ok {
					intValues[i] = intVal
				} else if floatVal, ok := v.(float64); ok {
					intValues[i] = int(floatVal)
				}
			}
			reduced = job.Reduce(keyStr, intValues)
		} else if job.ValueType() == "string" {
			strValues := make([]string, len(values))
			for i, v := range values {
				if strVal, ok := v.(string); ok {
					strValues[i] = strVal
				} else {
					strValues[i] = fmt.Sprintf("%v", v)
				}
			}
			reduced = job.Reduce(keyStr, strValues)
		} else {
			reduced = job.Reduce(keyStr, values)
		}

		if job.OutputFormat() == "kv" {
			output := common.OutputRecord{
				K: keyStr,
				V: reduced,
			}
			if err := encoder.Encode(output); err != nil {
				fmt.Fprintf(os.Stderr, "Error: Failed to encode output: %v\n", err)
				continue
			}
		} else {
			if err := encoder.Encode(reduced); err != nil {
				fmt.Fprintf(os.Stderr, "Error: Failed to encode output: %v\n", err)
				continue
			}
		}
		recordsWritten++
	}

	statsPath := outPath + ".stats.json"
	statsFile, err := os.Create(statsPath)
	if err == nil {
		stats := map[string]interface{}{
			"bytes_read":      bytesRead,
			"records_read":    recordsRead,
			"records_written": recordsWritten,
		}
		encoder := json.NewEncoder(statsFile)
		encoder.Encode(stats)
		statsFile.Close()
	}
}
