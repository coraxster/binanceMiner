FROM golang:1.12.4-alpine3.9 AS builder
RUN apk add git tzdata
ADD . /go/src/github.com/coraxster/binanceMiner
WORKDIR /go/src/github.com/coraxster/binanceMiner
RUN go get ./...

# https://github.com/docker/hub-feedback/issues/600
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags "-extldflags "-static" -X main.Version=$SOURCE_BRANCH($SOURCE_COMMIT)" -o /binanceMiner .

FROM scratch
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /binanceMiner ./
VOLUME /tmp/binanceMiner
ENTRYPOINT ["./binanceMiner"]