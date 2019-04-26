package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/coraxster/binanceScrubber"
	"github.com/labstack/gommon/log"
	"github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
	"time"
)

var chDsn = flag.String("clickhouse-dsn", "tcp://localhost:9000?username=default&compress=true", "clickhouse dsn")
var connN = flag.Int("binance-conn-n", 3, "binance connections number")
var fallbackPath = flag.String("fallback-path", "/tmp/binanceScrubber", "a place to store failed books")
var processFallback = flag.Bool("process-fallback", false, "process fallback and exit")

func main() {
	flag.Parse()
	conn, err := connectClickHouse(*chDsn)
	fatalOnErr(err, "connectClickHouse failed")
	log.Info("clickhouse connected")

	chStore := binanceScrubber.NewClickHouseStore(conn)
	fatalOnErr(err, "NewClickHouseStore failed")

	fbStore, err := binanceScrubber.NewFallbackStore(*fallbackPath)
	fatalOnErr(err, "NewFallbackStore failed")

	rec := binanceScrubber.NewReceiver(chStore, fbStore, 10000)

	if *processFallback {
		log.Info("process fallback starting")
		err = rec.ProcessFallback()
		fatalOnErr(err, "ProcessFallback failed")
		log.Info("process fallback done, exiting...")
		return
	}

	scrubber := binanceScrubber.NewBinanceScrubber()
	booksCh := make(chan *binanceScrubber.Book)
	err = seed(booksCh, scrubber, *connN)
	fatalOnErr(err, "seed books failed")
	log.Info("books seeder has been started")

	uniqueBooksCh := make(chan *binanceScrubber.Book)
	go unique(booksCh, uniqueBooksCh)

	for {
		err = rec.Receive(uniqueBooksCh)
		log.Warn(err)
	}
}

func connectClickHouse(dsn string) (*sql.DB, error) {
	conn, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(); err != nil {
		return nil, err
	}
	return conn, nil
}

func fatalOnErr(err error, msg string) {
	if err != nil {
		log.Fatal(errors.Wrap(err, msg))
	}
}

func seed(ch chan *binanceScrubber.Book, scrubber *binanceScrubber.BinanceScrubber, connN int) error {
	symbols, err := scrubber.GetAllSymbols()
	if err != nil {
		return err
	}
	worker := func(workerId int) {
		for {
			err := scrubber.SeedBooks(ch, symbols)
			if alive := scrubber.AliveCount(); alive > 0 {
				log.Warn("w:", workerId, ":", err, ". alive: ", alive, "/", connN)
			} else {
				log.Error("!!! w:", workerId, " ", err, ". alive: ", alive, "/", connN)
			}
			time.Sleep(2 * time.Second)
		}
	}
	for n := 1; n <= connN; n++ {
		go worker(n)
	}
	return nil
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
