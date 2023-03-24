FROM alpine:latest

RUN mkdir /app
RUN mkdir /internal
RUN mkdir /internal/tasks
RUN mkdir /internal/tasks/result-logs
RUN mkdir /internal/tasks/task-state

# COPY --from=builder /app/brokerApp /app
COPY sirius /app

CMD [ "/app/sirius" ]