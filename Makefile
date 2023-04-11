DOC_LOADER_SERVER=sirius
TASK_RESULT_PATH=./internal/task_result/result-logs
TASK_STATE_PATH=./internal/task_state/task_state_logs

run: build_dir build_sirius
	./${DOC_LOADER_SERVER}

clean deploy: clean_dir build_dir build_sirius_for_docker

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

build_sirius:
	@echo "Building Sirius"
	go build -o ${DOC_LOADER_SERVER} ./cmd/api

build_dir:
	mkdir -p ${TASK_RESULT_PATH}
	mkdir -p ${TASK_STATE_PATH}

clean_dir:
	rm -r ${TASK_RESULT_PATH}
	rm -r ${TASK_STATE_PATH}

build_sirius_for_docker:
	env GOOS=linux CGO_ENABLED=0 go build -o ${DOC_LOADER_SERVER} ./cmd/api

