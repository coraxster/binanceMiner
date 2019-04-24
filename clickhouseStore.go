package binanceScrubber

import (
	"database/sql"
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
		CREATE TABLE IF NOT EXISTS example_books (
			Symbol     String,
			SecN       UInt64,
			IsBid      UInt8,
			Price      Float64,
			Quantity   Float64
		) ENGINE=ReplacingMergeTree()
		  ORDER BY (Symbol, SecN, IsBid, Quantity)
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
	stmt, err := tx.Prepare("INSERT INTO example_books (Symbol, SecN, IsBid, Price, Quantity) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	for _, b := range books {
		for _, q := range b.Asks {
			_, err := stmt.Exec(b.Symbol, b.SecN, 0, q[0], q[1])
			if err != nil {
				return err
			}
		}
		for _, q := range b.Bids {
			_, err := stmt.Exec(b.Symbol, b.SecN, 1, q[0], q[1])
			if err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}
