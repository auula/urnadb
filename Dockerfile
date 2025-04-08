FROM golang:1.20-alpine AS builder

WORKDIR /app

COPY . .

RUN go build urnadb.go


FROM alpine:latest

LABEL maintainer="ding_ms@outlook.com"

WORKDIR /tmp/urnadb

COPY --from=builder /app/urnadb /usr/local/bin/urnadb

EXPOSE 2668

# ENTRYPOINT 可以让进程接受到 signal 信号，
# 区别于 CMD 不能正常接受到 signal 信号，CMD 命令回被覆盖
ENTRYPOINT ["/usr/local/bin/urnadb"]