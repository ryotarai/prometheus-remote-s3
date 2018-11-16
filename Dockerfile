FROM golang:1.11 AS builder
WORKDIR /go/src/app
COPY . .
RUN go build -o /usr/bin/prometheus-remote-s3 .

###############################################

FROM ubuntu:16.04
COPY --from=builder /usr/bin/prometheus-remote-s3 /usr/bin/prometheus-remote-s3
ENTRYPOINT ["/usr/bin/prometheus-remote-s3"]
