# Binance books miner
Pull books via websocket to clickhouse.

## Run
docker
```bash
docker run -it -v /tmp/binanceMiner:/tmp/binanceMiner corax/binance-miner:latest --clickhouse-dsn="tcp://host.docker.internal:9000?username=default&compress=true"
```

# Schema

```sql
create table IF NOT EXISTS books
(
    source String CODEC(Delta, ZSTD(5)),
    dt     DateTime CODEC(Delta, ZSTD(5)),
    secN   UInt64 CODEC(Delta, ZSTD(5)),
    symbol String CODEC(Delta, ZSTD(5)),
    asks Nested
        (
        price Float64,
        quantity Float64
        ) CODEC(Delta, ZSTD(5)),
    bids Nested
        (
        price Float64,
        quantity Float64
        ) CODEC(Delta, ZSTD(5))
) engine = ReplacingMergeTree()
      PARTITION BY (source, toYYYYMM(dt))
      ORDER BY (toYYYYMMDD(dt), symbol, secN);
```
