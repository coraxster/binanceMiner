package clickhouseStore

import (
	"github.com/pkg/errors"
	"time"
)

type Store interface {
	Store([]*Book) error
}

type FallbackStore interface {
	StoreOk([]*Book) error
	StoreToRetry([]*Book) error
	GetToRetry() (string, []*Book, error)
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
		select {
		case <-retryTicker.C:
			if err := rec.retryFailed(); err != nil {
				return errors.Wrap(err, "retryFailed failed")
			}
		case <-cleanupTicker.C:
			if err := rec.fbStore.CleanupOk(); err != nil {
				return errors.Wrap(err, "ok cleanup failed")
			}
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
