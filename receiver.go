package main

import (
	"github.com/coraxster/binanceMiner/clickhouseStore"
	"github.com/pkg/errors"
	"time"
)

type Store interface {
	Store([]*clickhouseStore.Book) error
}

type FallbackStore interface {
	StoreOk([]*clickhouseStore.Book) error
	StoreToRetry([]*clickhouseStore.Book) error
	GetToRetry() (string, []*clickhouseStore.Book, error)
	Delete(key string) error
	CleanupOk() error
}

type Receiver struct {
	mainStore Store
	fbStore   FallbackStore
	chunkSize int
}

var retryTicker = time.NewTicker(5 * time.Second)
var cleanupTicker = time.NewTicker(24 * time.Hour)

func NewReceiver(store Store, fbStore FallbackStore, chunkSize int) *Receiver {
	return &Receiver{store, fbStore, chunkSize}
}

func (rec *Receiver) Receive(ch chan *Book) error {
	buf := make([]*clickhouseStore.Book, 0, rec.chunkSize)
	for b := range ch {
		buf = append(buf, convert(b))
		if len(buf) < cap(buf) {
			continue
		}
		if err := rec.Store(buf); err != nil {
			return err
		}
		buf = buf[:0]
	}
	return nil
}

func convert(book *Book) *clickhouseStore.Book {
	askPrices := make([]float64, 0, len(book.Asks))
	askQuantities := make([]float64, 0, len(book.Asks))
	bidPrices := make([]float64, 0, len(book.Bids))
	bidQuantities := make([]float64, 0, len(book.Bids))
	for _, q := range book.Asks {
		askPrices = append(askPrices, q[0])
		askQuantities = append(askQuantities, q[1])
	}
	for _, q := range book.Bids {
		bidPrices = append(bidPrices, q[0])
		bidQuantities = append(bidQuantities, q[1])
	}
	return &clickhouseStore.Book{
		Source:        book.Source,
		Time:          book.Time,
		Symbol:        book.Symbol,
		SecN:          book.SecN,
		BidPrices:     bidPrices,
		AskPrices:     askPrices,
		BidQuantities: bidQuantities,
		AskQuantities: askQuantities,
	}
}

func (rec *Receiver) Store(books []*clickhouseStore.Book) error {
	err := rec.mainStore.Store(books)
	if err == nil {
		if err := rec.fbStore.StoreOk(books); err != nil {
			return errors.Wrap(err, "main stored, but fallback StoreOk failed :(")
		}
		return nil
	}
	if fbErr := rec.fbStore.StoreToRetry(books); fbErr != nil {
		return errors.Wrap(err, "main failed, fallback StoreToRetry also failed :(")
	}
	return err
}

func (rec *Receiver) MaintenanceWorker() error {
	for {
		<-retryTicker.C
		if err := rec.retryFailed(); err != nil {
			return errors.Wrap(err, "retryFailed failed")
		}
		<-cleanupTicker.C
		if err := rec.fbStore.CleanupOk(); err != nil {
			return errors.Wrap(err, "ok cleanup failed")
		}
	}
}

func (rec *Receiver) retryFailed() error {
	key, books, err := rec.fbStore.GetToRetry()
	if err != nil {
		return errors.Wrap(err, "GetToRetry failed :(")
	}
	if len(books) == 0 {
		return nil
	}
	if err = rec.mainStore.Store(books); err != nil {
		return errors.Wrap(err, "retry. main store failed :(")
	}
	if err = rec.fbStore.Delete(key); err != nil {
		return errors.Wrap(err, "retry. Delete failed :(")
	}
	if err := rec.fbStore.StoreOk(books); err != nil {
		return errors.Wrap(err, "retry main stored, but fallback StoreOk failed :(")
	}
	return nil
}
