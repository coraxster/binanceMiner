FROM golang:1.12.4-alpine3.9 AS builder
RUN apk add git
ADD . /go/src/github.com/coraxster/binanceScrubber
WORKDIR /go/src/github.com/coraxster/binanceScrubber/cmd
RUN go get ./...
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o /app main.go

FROM scratch
COPY --from=builder /app ./
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
VOLUME /tmp/binanceScrubber
ENTRYPOINT ["./app"]