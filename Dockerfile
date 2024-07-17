FROM golang:alpine as builder
WORKDIR project/
COPY . .
RUN ls -al
RUN go mod tidy
RUN  go build -o raft-cluster

FROM alpine
COPY --from=builder /go/project/raft-cluster .
ENV RAFT_PORT 8081
ENV HTTP_PORT 8082
ENV NODE_ID node1
ENV IFACE ens160
COPY run.sh .
RUN chmod +x run.sh
CMD ["./run.sh"]
