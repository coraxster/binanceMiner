package clickhouseStore

import (
	"compress/gzip"
	"encoding/gob"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

type LocalStore struct {
	okPath    string
	retryPath string
}

func NewLocalStore(path string) (*LocalStore, error) {
	okPath := path + "ok/"
	retryPath := path + "failed/"
	if err := makeDir(okPath); err != nil {
		return nil, err
	}
	if err := makeDir(retryPath); err != nil {
		return nil, err
	}
	return &LocalStore{okPath, retryPath}, nil
}

func makeDir(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err = os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return err
		}
	}
	if unix.Access(path, unix.W_OK) != nil {
		return errors.New("path is not writable")
	}
	return nil
}

func (s *LocalStore) StoreOk(books []*Book) error {
	return s.store(s.okPath, books)
}

func (s *LocalStore) StoreToRetry(books []*Book) error {
	return s.store(s.retryPath, books)
}

func (s *LocalStore) store(path string, books []*Book) error {
	filePath := fmt.Sprintf("%s/%s.gzip", path, time.Now().Format("2006-01-02T15:04:05.999999-07:00"))
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	zw := gzip.NewWriter(file)
	defer zw.Close()
	encoder := gob.NewEncoder(zw)
	return encoder.Encode(books)
}

func (s *LocalStore) GetToRetry() (string, []*Book, error) {
	filePaths, err := filepath.Glob(s.retryPath + "/*.gzip")
	if err != nil {
		return "", nil, err
	}
	if len(filePaths) == 0 {
		return "", nil, nil
	}
	fp := filePaths[0]
	books := make([]*Book, 0)
	file, err := os.Open(fp)
	if err != nil {
		return "", nil, err
	}
	defer file.Close()
	zr, err := gzip.NewReader(file)
	if err != nil {
		return "", nil, err
	}
	defer zr.Close()
	decoder := gob.NewDecoder(zr)
	err = decoder.Decode(&books)
	if err != nil {
		return "", nil, err
	}
	return fp, books, nil
}

func (s *LocalStore) Delete(path string) error {
	return os.Remove(path)
}

func (s *LocalStore) MoveToOk(path string) error {
	newPath := s.okPath + filepath.Base(path)
	return os.Rename(path, newPath)
}

func (s *LocalStore) CleanupOk(keepOk time.Duration) error {
	fileInfo, err := ioutil.ReadDir(s.okPath)
	if err != nil {
		return err
	}
	now := time.Now()
	for _, info := range fileInfo {
		if diff := now.Sub(info.ModTime()); diff < keepOk {
			continue
		}
		if err = os.Remove(s.okPath + info.Name()); err != nil {
			return err
		}
	}
	return nil
}
