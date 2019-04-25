package binanceScrubber

import (
	"database/sql"
	"github.com/kshvakov/clickhouse"
	_ "github.com/kshvakov/clickhouse"
)

type ClickHouseStore struct {
	conn      *sql.DB
	chunkSize int
}

func NewClickHouseStore(conn *sql.DB, chunkSize int) *ClickHouseStore {
	return &ClickHouseStore{conn, chunkSize}
}

func (chs *ClickHouseStore) Migrate() error {
	_, err := chs.conn.Exec(`
		create table IF NOT EXISTS example_books (
    symbol String,
    secN   UInt64 CODEC(Delta, ZSTD(5)),
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
) engine = ReplacingMergeTree() ORDER BY (symbol, secN)
	`)
	return err
}

//todo: сделать fallback хранилище.
func (chs *ClickHouseStore) Store(ch chan *Book) error {
	buf := make([]*Book, 0, chs.chunkSize)
	var err error
	for b := range ch {
		buf = append(buf, b)
		if len(buf) < cap(buf) {
			continue
		}
		err = chs.storeChunk(buf)
		if err != nil {
			return err
		}
		buf = buf[:0]
	}
	return nil
}

func (chs *ClickHouseStore) storeChunk(books []*Book) error {
	tx, err := chs.conn.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("insert into example_books_2 (symbol, secN, asks.price, asks.quantity, bids.price, bids.quantity) values (?, ?, ?, ?, ?, ?)")
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
			b.Symbol,
			b.SecN,
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
