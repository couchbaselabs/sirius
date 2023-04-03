FROM alpine:latest

RUN mkdir /app
RUN mkdir /internal
RUN mkdir /internal/task_state
RUN mkdir /internal/task_result
RUN mkdir /internal/task_result/result-logs
RUN mkdir /internal/task_state/task_state_logs

# COPY --from=builder /app/brokerApp /app
COPY sirius /app

CMD [ "/app/sirius" ]