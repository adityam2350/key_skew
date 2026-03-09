.PHONY: all build clean test master mapper reducer merge_unsalt make_zipf make_catastrophe

# Default target: build all binaries
all: build

# Build all binaries
build: master mapper reducer merge_unsalt make_zipf make_catastrophe

# Build individual binaries
master:
	@mkdir -p bin
	go build -o bin/master ./cmd/master

mapper:
	@mkdir -p bin
	go build -o bin/mapper ./cmd/mapper

reducer:
	@mkdir -p bin
	go build -o bin/reducer ./cmd/reducer

merge_unsalt:
	@mkdir -p bin
	go build -o bin/merge_unsalt ./cmd/merge_unsalt

make_zipf:
	@mkdir -p bin
	go build -o bin/make_zipf ./cmd/make_zipf

make_catastrophe:
	@mkdir -p bin
	go build -o bin/make_catastrophe ./cmd/make_catastrophe

# Run all unit tests
test:
	go test ./internal/...

# Clean build artifacts
clean:
	rm -rf bin/
	rm -f master mapper reducer merge_unsalt make_zipf make_catastrophe

