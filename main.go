package main

import (
	"flag"
	"github.com/coraxster/binanceMiner/clickhouseStore"
	"github.com/onrik/logrus/sentry"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"os"
	"time"
)

var Version = "0.0.0" // go build -ldflags "-X main.Version=0.0.1"

var chDsn = flag.String("clickhouse-dsn", "tcp://localhost:9000?username=default&compress=true", "clickhouse dsn")
var connN = flag.Int("binance-conn-n", 1, "binance connections number")
var chunkSize = flag.Int("chunk-size", 100000, "collect chunk-size then push to clickhouse, 100000 - about 30mb")
var fallbackPath = flag.String("fallback-path", "/tmp/binanceMiner/", "a place to store failed books")
var keepOkDays = flag.Int("keep-ok", 0, "how long keep sent books(days)")

var log = logrus.New()

func main() {
	//defer profile.Start(profile.MemProfile).Stop()

	log.Info("version: " + Version)
	if sentryDSN := os.Getenv("SENTRY_DSN"); sentryDSN != "" {
		sentryHook := sentry.NewHook(sentryDSN, logrus.PanicLevel, logrus.FatalLevel, logrus.ErrorLevel, logrus.WarnLevel)
		sentryHook.SetRelease(Version)
		log.AddHook(sentryHook)
		log.Info("sentry enabled")
	}
	flag.Parse()
	rec, err := clickhouseStore.NewReceiver(
		clickhouseStore.ReceiverConfig{
			ClickhouseDSN: *chDsn,
			ChunkSize:     *chunkSize,
			FallbackPath:  *fallbackPath,
			KeepOk:        time.Duration(*keepOkDays) * 24 * time.Hour,
		},
	)
	fatalOnErr(err, "NewReceiver failed")
	log.Info("clickhouseStore Receiver has been started")

	booksCh := make(chan *clickhouseStore.Book)
	seed(booksCh)
	log.Info("books seeder has been started")

	go func() {
		for {
			err := rec.Receive(booksCh)
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
	miner := NewBinanceMiner(20)
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
