FROM alpine:latest

RUN mkdir /app
RUN mkdir /internal
RUN mkdir /internal/task_result
RUN mkdir /internal/task_result/task_result_logs
RUN mkdir /internal/server_requests
RUN mkdir /internal/server_requests/server_requests_logs
RUN mkdir /internal/tasks
RUN mkdir /internal/tasks/request_logs


# COPY --from=builder /app/brokerApp /app
COPY sirius /app

CMD [ "/app/sirius" ]