# Makefile — збірка xsink.

OUT_DIR := bin
BIN     := xsink

.PHONY: build build-linux tidy test clean

build:
	mkdir -p $(OUT_DIR)
	go build -trimpath -o $(OUT_DIR)/$(BIN) ./cmd/xsink

# Крос-компіляція під Linux amd64 (для деплою на прод).
build-linux:
	mkdir -p $(OUT_DIR)/linux
	GOOS=linux GOARCH=amd64 go build -trimpath -o $(OUT_DIR)/linux/$(BIN) ./cmd/xsink

tidy:
	go mod tidy

test:
	go test ./...

clean:
	rm -rf $(OUT_DIR)

# Друк DDL з mapping (для перевірки перед прод-деплоєм).
print-ddl:
	go run ./cmd/xsink --config config.yaml --print-ddl
