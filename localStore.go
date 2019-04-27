package binanceScrubber

import (
	"encoding/gob"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"time"
)

type LocalStore struct {
	path string
}

func NewLocalStore(path string) (*LocalStore, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err = os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}
	if unix.Access(path, unix.W_OK) != nil {
		return nil, errors.New("path is not writable")
	}
	return &LocalStore{path}, nil
}

func (s *LocalStore) Store(books []*Book) error {
	filePath := fmt.Sprintf("%s/%s.data", s.path, time.Now().Format("2006-01-02T15:04:05.999999-07:00"))
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := gob.NewEncoder(file)
	return encoder.Encode(books)
}

func (s *LocalStore) Get() (string, []*Book, error) {
	filePaths, err := filepath.Glob(s.path + "/*.data")
	if err != nil {
		return "", nil, nil
	}
	books := make([]*Book, 0)
	for _, fp := range filePaths {
		file, err := os.Open(fp)
		if err != nil {
			return "", nil, err
		}
		defer file.Close()
		decoder := gob.NewDecoder(file)
		err = decoder.Decode(&books)
		if err != nil {
			return "", nil, err
		}
		return fp, books, nil
	}
	return "", nil, nil
}

func (s *LocalStore) Delete(path string) error {
	return os.Remove(path)
}
