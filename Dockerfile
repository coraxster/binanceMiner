FROM golang:1.12.4-alpine3.9 AS builder
RUN apk add git tzdata
ADD . /go/src/github.com/coraxster/binanceMiner
WORKDIR /go/src/github.com/coraxster/binanceMiner
RUN go get ./...
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o /app .

FROM scratch
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /app ./
VOLUME /tmp/binanceMiner
ENTRYPOINT ["./app"]