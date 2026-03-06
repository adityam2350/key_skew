package main

import (
	"fmt"
	"os"

	pagerankmaster "key_skew/pagerank/master"
	wordcountmaster "key_skew/wordcount/master"
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
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n", subcommand)
		fmt.Fprintf(os.Stderr, "Usage: %s <run|run-pagerank> [flags]\n", os.Args[0])
		os.Exit(1)
	}
}
