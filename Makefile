DOC_LOADER_SERVER=sirius

run: up_build
	./${DOC_LOADER_SERVER}

deploy: build_sirius_for_docker
	@echo "Stopping docker images (if running...)"
	docker-compose down
	@echo "Building (when required) and starting docker images..."
	docker-compose up --build -d
	@echo "Docker images built and started!"

down:
	@echo "Stopping docker images"
	docker-compose down
	@echo "Stopped docker images"

up_build: build_sirius

build_sirius:
	@echo "Building Sirius"
	go build -o ${DOC_LOADER_SERVER} ./cmd/api

build_sirius_for_docker:
	env GOOS=linux CGO_ENABLED=0 go build -o ${DOC_LOADER_SERVER} ./cmd/api