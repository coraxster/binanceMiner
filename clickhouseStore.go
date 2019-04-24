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

func NewClickHouseStore(conn *sql.DB, chunkSize int) (*ClickHouseStore, error) {
	return &ClickHouseStore{conn, chunkSize}, nil
}

func (chs *ClickHouseStore) Migrate() error {
	_, err := chs.conn.Exec(`
		create table IF NOT EXISTS example_books (
    symbol String,
    secN   UInt64 CODEC(Delta, ZSTD(5)),
    quote Nested
        (
        isBid Int8,
        price Float64,
        quantity Float64
        ) CODEC(Delta, ZSTD(5))
) engine = ReplacingMergeTree() ORDER BY (symbol, secN)
	`)
	return err
}

//todo: попробовать схему с хранением буков в массиве, сравнить место
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
	stmt, err := tx.Prepare("insert into example_books (symbol, secN, quote.isBid, quote.price, quote.quantity) values (?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	for _, b := range books {
		isBids := make([]int, 0, len(b.Bids)+len(b.Asks))
		prices := make([]float64, 0, len(b.Bids)+len(b.Asks))
		quantities := make([]float64, 0, len(b.Bids)+len(b.Asks))
		for _, q := range b.Bids {
			isBids = append(isBids, 1)
			prices = append(prices, q[0])
			quantities = append(quantities, q[1])
		}
		for _, q := range b.Asks {
			isBids = append(isBids, 0)
			prices = append(prices, q[0])
			quantities = append(quantities, q[1])
		}
		_, err := stmt.Exec(b.Symbol, b.SecN, clickhouse.Array(isBids), clickhouse.Array(prices), clickhouse.Array(quantities))
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}
