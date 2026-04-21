FROM golang:1.24-alpine AS builder

WORKDIR /app

# 拷贝依赖文件（如果你有 go.mod / go.sum，这一步很关键）
COPY go.mod go.sum ./
RUN go mod download

# 拷贝源码
COPY . .

# 构建二进制（建议加 -o 明确输出文件）
RUN go build -o urnadb urnadb.go


FROM alpine:latest

LABEL maintainer="ding_ms@outlook.com"

WORKDIR /tmp/urnadb

# 只拷贝编译好的二进制
COPY --from=builder /app/urnadb /usr/local/bin/urnadb

EXPOSE 2668

# ENTRYPOINT 保证信号可正确传递（适用于数据库/服务进程）
ENTRYPOINT ["/usr/local/bin/urnadb"]