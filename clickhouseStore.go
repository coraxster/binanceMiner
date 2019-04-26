package binanceScrubber

import (
	"database/sql"
	"github.com/kshvakov/clickhouse"
	_ "github.com/kshvakov/clickhouse"
	"github.com/pkg/errors"
)

type ClickHouseStore struct {
	conn      *sql.DB
	chunkSize int
	fbStore   *FallbackStore
}

func NewClickHouseStore(conn *sql.DB, chunkSize int, fbStore *FallbackStore) *ClickHouseStore {
	return &ClickHouseStore{conn, chunkSize, fbStore}
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
  ORDER BY (symbol, secN)
	`)
	return err
}

func (chs *ClickHouseStore) Receive(ch chan *Book) error {
	handleErr := func(err error, books []*Book) error {
		fbErr := chs.fbStore.Store(books)
		if fbErr != nil {
			return errors.Wrap(err, "fallback also failed :(")
		}
		return err
	}
	buf := make([]*Book, 0, chs.chunkSize)
	var err error
	for b := range ch {
		buf = append(buf, b)
		if len(buf) < cap(buf) {
			continue
		}
		err = chs.Store(buf)
		if err != nil {
			return handleErr(err, buf)
		}
		buf = buf[:0]
	}
	return nil
}

func (chs *ClickHouseStore) Store(books []*Book) error {
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

func (chs *ClickHouseStore) StoreFallback() error {
	for {
		key, books, err := chs.fbStore.Get()
		if err != nil {
			return err
		}
		if len(books) == 0 {
			return nil
		}
		err = chs.Store(books)
		if err != nil {
			return err
		}
		err = chs.fbStore.Delete(key)
		if err != nil {
			return err
		}
	}
}
