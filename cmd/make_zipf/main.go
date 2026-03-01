package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"

	"key_skew/internal/common"
)

func main() {
	var nRecords int
	var vocabSize int
	var zipfS float64
	var zipfV float64
	var wordsPerRecord int
	var seed int64
	var outPath string

	flag.IntVar(&nRecords, "N-records", 10000, "Number of records to generate")
	flag.IntVar(&vocabSize, "vocab-size", 1000, "Vocabulary size")
	flag.Float64Var(&zipfS, "zipf-s", 1.1, "Zipf distribution parameter s")
	flag.Float64Var(&zipfV, "zipf-v", 1.0, "Zipf distribution parameter v")
	flag.IntVar(&wordsPerRecord, "words-per-record", 20, "Number of words per record")
	flag.Int64Var(&seed, "seed", 0, "Random seed")
	flag.StringVar(&outPath, "out", "data/zipf.jsonl", "Output file path")
	flag.Parse()

	// Initialize logging
	if err := common.InitLogging(".", "make_zipf"); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logging: %v\n", err)
		os.Exit(1)
	}
	defer common.CloseLogging()

	common.LogInfo("MAKE_ZIPF", "Generating Zipf dataset: N=%d, vocab=%d, s=%.2f, v=%.2f, words/record=%d, seed=%d",
		nRecords, vocabSize, zipfS, zipfV, wordsPerRecord, seed)

	// Create output directory
	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		common.LogError("MAKE_ZIPF", "Failed to create output directory: %v", err)
		os.Exit(1)
	}

	// Create output file
	file, err := os.Create(outPath)
	if err != nil {
		common.LogError("MAKE_ZIPF", "Failed to create output file: %v", err)
		os.Exit(1)
	}
	defer file.Close()

	// Initialize random number generator
	rng := rand.New(rand.NewSource(seed))

	// Create Zipf distribution
	zipf := rand.NewZipf(rng, zipfS, zipfV, uint64(vocabSize-1))

	// Generate vocabulary
	vocab := make([]string, vocabSize)
	for i := 0; i < vocabSize; i++ {
		vocab[i] = fmt.Sprintf("w%06d", i+1)
	}

	// Generate records
	encoder := json.NewEncoder(file)
	for i := 0; i < nRecords; i++ {
		words := make([]string, wordsPerRecord)
		for j := 0; j < wordsPerRecord; j++ {
			idx := zipf.Uint64()
			words[j] = vocab[idx]
		}

		record := common.InputRecord{
			Text: fmt.Sprintf("%s", words[0]),
		}
		for j := 1; j < len(words); j++ {
			record.Text += " " + words[j]
		}

		if err := encoder.Encode(record); err != nil {
			common.LogError("MAKE_ZIPF", "Failed to encode record %d: %v", i, err)
			os.Exit(1)
		}
	}

	common.LogInfo("MAKE_ZIPF", "Generated %d records to %s", nRecords, outPath)
}
