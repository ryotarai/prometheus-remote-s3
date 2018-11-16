FROM golang:1.11 AS builder
WORKDIR /go/src/app
COPY . .
RUN go build -o /usr/bin/prometheus-remote-s3 .

###############################################

FROM ubuntu:16.04
COPY --from=builder /usr/bin/prometheus-remote-s3 /usr/bin/prometheus-remote-s3
RUN apt-get update && apt-get install -y \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["/usr/bin/prometheus-remote-s3"]
