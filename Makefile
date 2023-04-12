DOC_LOADER_SERVER=sirius
TASK_RESULT_PATH=./internal/task_result/result-logs
TASK_STATE_PATH=./internal/task_state/task_state_logs

run: build_dir build_sirius
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

build_sirius:
	@echo "Building Sirius"
	go build -o ${DOC_LOADER_SERVER} ./cmd/api

build_dir:
	@echo "Building directory to store task's meta-data and results"
	mkdir -p ${TASK_RESULT_PATH}
	mkdir -p ${TASK_STATE_PATH}

clean_dir:
	@echo "Clean meta-data of task state and task results"
	if [ -d ${TASK_RESULT_PATH} ]; then rm -Rf ${TASK_RESULT_PATH}; fi
	if [ -d ${TASK_STATE_PATH} ]; then rm -Rf ${TASK_STATE_PATH}; fi

build_sirius_for_docker:
	env GOOS=linux CGO_ENABLED=0 go build -o ${DOC_LOADER_SERVER} ./cmd/api

clean_run: clean_dir run

clean_deploy: clean_dir build_dir deploy
