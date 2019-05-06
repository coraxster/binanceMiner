package clickhouseStore

import (
	"github.com/pkg/errors"
	"runtime"
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
	CleanupOk(time.Duration) error
	MoveToOk(path string) error
}

type Receiver struct {
	mainStore Store
	fbStore   FallbackStore
	chunkSize int
	keepOk    time.Duration
}

type ReceiverConfig struct {
	ClickhouseDSN string
	ChunkSize     int
	FallbackPath  string
	KeepOk        time.Duration
}

var retryTicker = time.NewTicker(5 * time.Second)
var cleanupTicker = time.NewTicker(24 * time.Hour)

func NewReceiver(config ReceiverConfig) (*Receiver, error) {
	chStore, err := NewClickHouseStore(config.ClickhouseDSN)
	if err != nil {
		return nil, errors.Wrap(err, "NewClickHouseStore failed")
	}
	if err = chStore.Migrate(); err != nil {
		return nil, errors.Wrap(err, "ClickHouseStore migrate failed")
	}

	fbStore, err := NewLocalStore(config.FallbackPath)
	if err != nil {
		return nil, errors.Wrap(err, "NewLocalStore failed")
	}
	return &Receiver{chStore, fbStore, config.ChunkSize, config.KeepOk}, nil
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
	defer runtime.GC()
	err := rec.mainStore.Store(books)
	if err != nil {
		if fbErr := rec.fbStore.StoreToRetry(books); fbErr != nil {
			return errors.Wrap(err, "main failed, fallback StoreToRetry also failed :(")
		}
		return errors.Wrap(err, "main failed, fallback stored")
	}
	if rec.keepOk == 0 {
		return nil
	}
	if err := rec.fbStore.StoreOk(books); err != nil {
		return errors.Wrap(err, "main stored, but fallback StoreOk failed :(")
	}
	return nil
}

func (rec *Receiver) MaintenanceWorker() error {
	for {
		select {
		case <-retryTicker.C:
			if err := rec.retryFailed(); err != nil {
				return errors.Wrap(err, "retryFailed failed")
			}
		case <-cleanupTicker.C:
			if rec.keepOk == 0 {
				continue
			}
			if err := rec.fbStore.CleanupOk(rec.keepOk); err != nil {
				return errors.Wrap(err, "CleanupOk failed")
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
	if err = rec.fbStore.MoveToOk(key); err != nil {
		return errors.Wrap(err, "retry. MoveToOk failed :(")
	}
	return nil
}
