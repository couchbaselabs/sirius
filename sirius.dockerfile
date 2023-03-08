FROM alpine:latest

RUN mkdir /app

# COPY --from=builder /app/brokerApp /app
COPY sirius /app

CMD [ "/app/sirius" ]