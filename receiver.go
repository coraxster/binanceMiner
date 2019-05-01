package main

import (
	"github.com/coraxster/binanceScrubber/clickhouseStore"
	"github.com/pkg/errors"
	"time"
)

type Store interface {
	Store([]*clickhouseStore.Book) error
}

type FallbackStore interface {
	Store([]*clickhouseStore.Book) error
	Get() (string, []*clickhouseStore.Book, error)
	Delete(key string) error
}

type Receiver struct {
	mainStore Store
	fbStore   FallbackStore
	chunkSize int
}

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
		return nil
	}
	fbErr := rec.fbStore.Store(books)
	if fbErr != nil {
		return errors.Wrap(err, "fallback also failed :(")
	}
	return err
}

func (rec *Receiver) MaintenanceWorker(sleep time.Duration) error {
	for {
		time.Sleep(sleep)
		if err := rec.processFallback(); err != nil {
			return err
		}
		if err := rec.cleanup(); err != nil {
			return err
		}
	}
}

func (rec *Receiver) processFallback() error {
	key, books, err := rec.fbStore.Get()
	if err != nil {
		return err
	}
	if len(books) == 0 {
		return nil
	}
	err = rec.mainStore.Store(books)
	if err != nil {
		return err
	}
	err = rec.fbStore.Delete(key)
	if err != nil {
		return err
	}
	return nil
}

func (rec *Receiver) cleanup() error {
	return nil
}
