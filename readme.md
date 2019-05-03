# Binance books miner
Pull books via websocket to clickhouse.

## Run
docker
```bash
docker run -it -v /tmp/binanceMiner:/tmp/binanceMiner corax/binance-miner:latest --clickhouse-dsn="tcp://host.docker.internal:9000?username=default&compress=true"
```
