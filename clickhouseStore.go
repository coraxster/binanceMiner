package main

import (
	"database/sql"
	"github.com/kshvakov/clickhouse"
	"sort"
)

type ClickHouseStore struct {
	conn *sql.DB
}

func NewClickHouseStore(dsn string) (*ClickHouseStore, error) {
	conn, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(); err != nil {
		return nil, err
	}
	return &ClickHouseStore{conn}, nil
}

func (chs *ClickHouseStore) Migrate() error {
	_, err := chs.conn.Exec(`
create table IF NOT EXISTS books (
	source String CODEC(Delta, ZSTD(5)),
	dt   DateTime CODEC(Delta, ZSTD(5)),
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
  ORDER BY (toYYYYMMDD(dt), symbol, secN)
	`)
	return err
}

func (chs *ClickHouseStore) Store(books []*Book) error {
	sortBooks(books) // sort here to reduce clickhouse load
	tx, err := chs.conn.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("insert into books (source, dt, secN, symbol, asks.price, asks.quantity, bids.price, bids.quantity) values (?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	askPrices := make([]float64, 0, len(books[0].Asks))
	askQuantities := make([]float64, 0, len(books[0].Asks))
	bidPrices := make([]float64, 0, len(books[0].Bids))
	bidQuantities := make([]float64, 0, len(books[0].Bids))
	for _, b := range books {
		for _, q := range b.Asks {
			askPrices = append(askPrices, q[0])
			askQuantities = append(askQuantities, q[1])
		}
		for _, q := range b.Bids {
			bidPrices = append(bidPrices, q[0])
			bidQuantities = append(bidQuantities, q[1])
		}
		_, err := stmt.Exec(
			b.Source,
			clickhouse.DateTime(b.Time),
			b.SecN,
			b.Symbol,
			clickhouse.Array(askPrices),
			clickhouse.Array(askQuantities),
			clickhouse.Array(bidPrices),
			clickhouse.Array(bidQuantities),
		)
		if err != nil {
			return err
		}
		askPrices = askPrices[:0]
		askQuantities = askQuantities[:0]
		bidPrices = bidPrices[:0]
		bidQuantities = bidQuantities[:0]
	}
	return tx.Commit()
}

// according clickhouse key "20060102absxyz123123"
func sortBooks(books []*Book) {
	sort.Slice(books, func(i, j int) bool {
		iY, iM, iD := books[i].Time.Date()
		jY, jM, jD := books[j].Time.Date()
		if iY < jY {
			return true
		}
		if iY > jY {
			return false
		}

		if iM < jM {
			return true
		}
		if iM > jM {
			return false
		}

		if iD < jD {
			return true
		}
		if iD > jD {
			return false
		}

		if books[i].Symbol < books[j].Symbol {
			return true
		}
		if books[i].Symbol > books[j].Symbol {
			return false
		}

		return books[i].SecN < books[j].SecN
	})
}
