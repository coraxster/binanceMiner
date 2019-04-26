package binanceScrubber

import (
	"encoding/gob"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"time"
)

type FallbackStore struct {
	path string
}

func NewFallbackStore(path string) (*FallbackStore, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err = os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}
	if unix.Access(path, unix.W_OK) != nil {
		return nil, errors.New("path is not writable")
	}
	return &FallbackStore{path}, nil
}

func (s *FallbackStore) Store(books []*Book) error {
	fmt.Print("writing...")
	filePath := fmt.Sprintf("%s/%s.data", s.path, time.Now().Format("2006-01-02T15:04:05.999999-07:00"))
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := gob.NewEncoder(file)
	return encoder.Encode(books)
}

func (s *FallbackStore) Get() (string, []*Book, error) {
	return "", nil, nil
}

func (s *FallbackStore) Delete(key string) error {
	return nil
}
