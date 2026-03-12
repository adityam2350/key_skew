package main

import (
	"fmt"
	"os"

	pagerankmaster "key_skew/pagerank/master"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <run-pagerank> [flags]\n", os.Args[0])
		os.Exit(1)
	}

	subcommand := os.Args[1]
	if subcommand != "run-pagerank" {
		fmt.Fprintf(os.Stderr, "Usage: %s <run-pagerank> [flags]\n", os.Args[0])
		os.Exit(1)
	}

	pagerankmaster.RunPageRank()
}
