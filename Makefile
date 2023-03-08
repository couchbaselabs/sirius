DOC_LOADER_SERVER=sirius

run: up_build
	./${DOC_LOADER_SERVER}

up_build: build_sirius

build_sirius:
	@echo "Building Sirius"
	go build -o ${DOC_LOADER_SERVER} ./cmd/api
