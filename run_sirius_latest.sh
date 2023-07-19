#!/bin/bash

TASK_RESULT_PATH=./internal/task_result/task_result_logs
TASK_STATE_PATH=./internal/task_state/task_state_logs
SERVER_REQUESTS_PATH=./internal/server_requests/server_requests_logs
TASK_REQUEST_PATH=./internal/tasks/request_logs

echo "Clean meta-data of task state and task results"
if [ -d ${TASK_RESULT_PATH} ]; then rm -Rf ${TASK_RESULT_PATH}; fi
if [ -d ${TASK_STATE_PATH} ]; then rm -Rf ${TASK_STATE_PATH}; fi
if [ -d ${TASK_REQUEST_PATH} ]; then rm -Rf ${TASK_REQUEST_PATH}; fi
if [ -d ${SERVER_REQUESTS_PATH} ]; then rm -Rf ${SERVER_REQUESTS_PATH}; fi
echo "Building directory to store task's meta-data and results"
mkdir -p ${TASK_RESULT_PATH}
mkdir -p ${TASK_STATE_PATH}
mkdir -p ${SERVER_REQUESTS_PATH}
mkdir -p ${TASK_REQUEST_PATH}

docker container stop sirius
docker container rm sirius

docker run -d -p 4000:4000 \
-v "$(pwd)/internal/tasks/request_logs:/internal/tasks/request_logs" \
-v "$(pwd)/internal/task_result/task_result_logs:/internal/task_result/task_result_logs" \
-v "$(pwd)/internal/server_requests/server_requests_logs:/internal/server_requests/server_requests_logs" \
-v "$(pwd)/internal/task_state/task_state_logs:/internal/task_state/task_state_logs" \
--pull=always --name sirius  -d sequoiatools/sirius:latest