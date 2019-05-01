package main

import (
	"flag"
	"fmt"
	"github.com/coraxster/binanceScrubber/clickhouseStore"
	"github.com/labstack/gommon/log"
	"github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
	"time"
)

var chDsn = flag.String("clickhouse-dsn", "tcp://localhost:9000?username=default&compress=true", "clickhouse dsn")
var connN = flag.Int("binance-conn-n", 2, "binance connections number")
var chunkSize = flag.Int("chunk-size", 100000, "collect chunk-size then push to clickhouse, 100000 - about 30mb")
var fallbackPath = flag.String("fallback-path", "/tmp/binanceScrubber", "a place to store failed books")
var processFallbackSleep = flag.Int("process-fallback-sleep", 30, "process fallback sleep between chunks")

func main() {
	flag.Parse()

	chStore, err := clickhouseStore.NewClickHouseStore(*chDsn)
	fatalOnErr(err, "NewClickHouseStore failed")
	fatalOnErr(chStore.Migrate(), "ClickHouseStore migrate failed")

	fbStore, err := clickhouseStore.NewLocalStore(*fallbackPath)
	fatalOnErr(err, "NewLocalStore failed")

	rec := NewReceiver(chStore, fbStore, *chunkSize)

	booksCh := make(chan *Book)
	fatalOnErr(seed(booksCh), "seed books failed")
	log.Info("books seeder has been started")

	uniqueCh := unique(booksCh)
	go func() {
		for {
			err := rec.Receive(uniqueCh)
			log.Warn("receive error: " + err.Error())
		}
	}()

	for {
		err = rec.MaintenanceWorker(time.Duration(*processFallbackSleep) * time.Second)
		log.Warn("MaintenanceWorker error: " + err.Error())
	}
}

func fatalOnErr(err error, msg string) {
	if err != nil {
		log.Fatal(errors.Wrap(err, msg))
	}
}

func seed(ch chan *Book) error {
	scrubber := NewBinanceScrubber()
	symbols, err := scrubber.GetAllSymbols()
	if err != nil {
		return err
	}
	worker := func(workerId int) {
		for {
			err := scrubber.SeedBooks(ch, symbols)
			if alive := scrubber.AliveCount(); alive > 0 {
				log.Warn("w:", workerId, ":", err, ". alive: ", alive, "/", *connN)
			} else {
				log.Error("!!! w:", workerId, " ", err, ". alive: ", alive, "/", *connN)
			}
			time.Sleep(2 * time.Second)
		}
	}
	for n := 1; n <= *connN; n++ {
		go worker(n)
	}
	return nil
}

func unique(in chan *Book) chan *Book {
	out := make(chan *Book, 30000) // about 30000 books/min in. clickhouse write timeout = 1 min
	c := cache.New(10*time.Minute, 20*time.Minute)
	go func() {
		for b := range in {
			key := fmt.Sprint(b.Symbol, b.SecN)
			if _, exists := c.Get(key); exists {
				continue
			}
			c.Set(key, struct{}{}, cache.DefaultExpiration)
			out <- b
		}
	}()
	return out
}
