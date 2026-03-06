package main

import (
	"fmt"
	"os"

	wordcountmaster "key_skew/wordcount/master"
	pagerankmaster "key_skew/pagerank/master"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <run|run-pagerank> [flags]\n", os.Args[0])
		os.Exit(1)
	}

	subcommand := os.Args[1]
	switch subcommand {
	case "run":
		wordcountmaster.RunWordCount()
	case "run-pagerank":
		pagerankmaster.RunPageRank()
	default:
		fmt.Fprintf(os.Stderr, "Usage: %s <run|run-pagerank> [flags]\n", os.Args[0])
		os.Exit(1)
	}
}
