FROM alpine:latest

RUN mkdir /app
RUN mkdir /results/
RUN mkdir /results/result-logs

# COPY --from=builder /app/brokerApp /app
COPY sirius /app

CMD [ "/app/sirius" ]