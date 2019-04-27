package main

import "github.com/pkg/errors"

type Store interface {
	Store([]*Book) error
}

type FallbackStore interface {
	Store([]*Book) error
	Get() (string, []*Book, error)
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
	buf := make([]*Book, 0, rec.chunkSize)
	for b := range ch {
		buf = append(buf, b)
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
func (rec *Receiver) Store(books []*Book) error {
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

func (rec *Receiver) ProcessFallback() error {
	for {
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
	}
}
