# Binance books scrubber
Pull books via websocket to clickhouse.

## Run
docker
```bash
docker run -it corax/binance-scrubber:latest --clickhouse-dsn="tcp://host.docker.internal:9000?username=default&compress=true"
```
