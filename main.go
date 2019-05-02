package main

import (
	"flag"
	"fmt"
	"github.com/coraxster/binanceMiner/clickhouseStore"
	"github.com/labstack/gommon/log"
	"github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
	"github.com/pkg/profile"
	"time"
)

var chDsn = flag.String("clickhouse-dsn", "tcp://localhost:9000?username=default&compress=true", "clickhouse dsn")
var connN = flag.Int("binance-conn-n", 2, "binance connections number")
var chunkSize = flag.Int("chunk-size", 100000, "collect chunk-size then push to clickhouse, 100000 - about 30mb")
var fallbackPath = flag.String("fallback-path", "/tmp/binanceMiner/", "a place to store failed books")
var keepOkDays = flag.Int("keep-ok", 7, "how long keep sent books(days)")

func main() {
	p := profile.Start(profile.MemProfile)
	go func() {
		time.Sleep(time.Minute * 10)
		p.Stop()
		log.Fatal("Done")
	}()

	flag.Parse()

	rec, err := clickhouseStore.NewReceiver(
		*chDsn,
		*chunkSize,
		*fallbackPath,
		time.Duration(*keepOkDays)*24*time.Hour,
	)
	fatalOnErr(err, "NewReceiver failed")
	log.Info("clickhouseStore Receiver has been started")

	booksCh := make(chan *clickhouseStore.Book)
	seed(booksCh)
	log.Info("books seeder has been started")

	uniqueClickBooksCh := unique(booksCh)
	go func() {
		for {
			err := rec.Receive(uniqueClickBooksCh)
			log.Warn("receive error: " + err.Error())
		}
	}()

	for {
		err = rec.MaintenanceWorker()
		log.Warn("MaintenanceWorker error: " + err.Error())
	}
}

func fatalOnErr(err error, msg string) {
	if err != nil {
		log.Fatal(errors.Wrap(err, msg))
	}
}

func seed(ch chan *clickhouseStore.Book) {
	miner := NewBinanceMiner()
	symbols, err := miner.GetAllSymbols()
	fatalOnErr(err, "get symbols failed")
	worker := func(workerId int) {
		for {
			err := miner.SeedBooks(ch, symbols)
			if alive := miner.AliveCount(); alive > 0 {
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
}

func unique(in chan *clickhouseStore.Book) chan *clickhouseStore.Book {
	out := make(chan *clickhouseStore.Book, 30000) // about 30000 books/min in. clickhouse write timeout = 1 min
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
