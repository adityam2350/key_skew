.PHONY: all build clean master mapper reducer merge_unsalt make_zipf make_catastrophe init_pagerank make_zipf_graph make_skewed_graph merge_unsalt_pagerank analyze_heavy

# Default target: build all binaries
all: build

# Build all binaries
build: master mapper reducer merge_unsalt make_zipf make_catastrophe init_pagerank make_zipf_graph make_skewed_graph merge_unsalt_pagerank analyze_heavy

# Build individual binaries
master:
	@mkdir -p bin
	go build -o bin/master ./common/cmd/master

mapper:
	@mkdir -p bin
	go build -o bin/mapper ./common/cmd/mapper

reducer:
	@mkdir -p bin
	go build -o bin/reducer ./common/cmd/reducer

merge_unsalt:
	@mkdir -p bin
	go build -o bin/merge_unsalt ./wordcount/cmd/merge_unsalt

make_zipf:
	@mkdir -p bin
	go build -o bin/make_zipf ./wordcount/cmd/make_zipf

make_catastrophe:
	@mkdir -p bin
	go build -o bin/make_catastrophe ./wordcount/cmd/make_catastrophe

init_pagerank:
	@mkdir -p bin
	go build -o bin/init_pagerank ./pagerank/cmd/init_pagerank

make_zipf_graph:
	@mkdir -p bin
	go build -o bin/make_zipf_graph ./pagerank/cmd/make_zipf_graph

make_skewed_graph:
	@mkdir -p bin
	go build -o bin/make_skewed_graph ./pagerank/cmd/make_skewed_graph

merge_unsalt_pagerank:
	@mkdir -p bin
	go build -o bin/merge_unsalt_pagerank ./pagerank/cmd/merge_unsalt

analyze_heavy:
	@mkdir -p bin
	go build -o bin/analyze_heavy ./pagerank/cmd/analyze_heavy

# Clean build artifacts
clean:
	rm -rf bin/
	rm -f master mapper reducer merge_unsalt make_zipf make_catastrophe init_pagerank make_zipf_graph make_skewed_graph

