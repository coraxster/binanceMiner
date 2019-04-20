package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"github.com/coraxster/binanceScrubber"
	"github.com/labstack/gommon/log"
	"github.com/patrickmn/go-cache"
	"time"
)

var chDsn = flag.String("clickhouse-dsn", "tcp://localhost:9000?username=default&compress=true", "clickhouse dsn")

func main() {
	scrubber := binanceScrubber.NewBinanceScrubber()
	conn, err := connectClickHouse()
	if err != nil {
		log.Fatal(err)
	}
	log.Info("clickhouse connected")
	store, err := binanceScrubber.NewClickHouseStore(conn, 100)
	if err != nil {
		log.Fatal(err)
	}
	booksCh := make(chan *binanceScrubber.Book)
	go seed(booksCh, scrubber)
	//go seed(booksCh, scrubber)
	log.Info("books seed started")

	uniqueBooksCh := make(chan *binanceScrubber.Book)
	go unique(booksCh, uniqueBooksCh)

	for {
		err = store.Store(uniqueBooksCh)
		log.Warn(err)
	}
}

func connectClickHouse() (*sql.DB, error) {
	conn, err := sql.Open("clickhouse", *chDsn)
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(); err != nil {
		return nil, err
	}
	return conn, nil
}

func seed(ch chan *binanceScrubber.Book, scrubber *binanceScrubber.BinanceScrubber) {
	ctx := context.Background()
	for {
		err := scrubber.SeedBooks(ctx, ch)
		log.Warn(err)
		time.Sleep(2 * time.Second)
	}
}

// возможно лучше встроить в scrubber, но это неточно
func unique(in chan *binanceScrubber.Book, out chan *binanceScrubber.Book) {
	c := cache.New(10*time.Minute, 20*time.Minute)
	for b := range in {
		key := fmt.Sprint(b.Symbol, b.SecN)
		if _, exists := c.Get(key); exists {
			continue
		}
		c.Set(key, struct{}{}, cache.DefaultExpiration)
		out <- b
	}
}
