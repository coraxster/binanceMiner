package clickhouseStore

import (
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
	keepOk    time.Duration
}

func NewLocalStore(path string, keepOk time.Duration) (*LocalStore, error) {
	okPath := path + "ok/"
	retryPath := path + "failed/"
	if err := makeDir(okPath); err != nil {
		return nil, err
	}
	if err := makeDir(retryPath); err != nil {
		return nil, err
	}
	return &LocalStore{okPath, retryPath, keepOk}, nil
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
	filePath := fmt.Sprintf("%s/%s.data", path, time.Now().Format("2006-01-02T15:04:05.999999-07:00"))
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := gob.NewEncoder(file)
	return encoder.Encode(books)
}

func (s *LocalStore) GetToRetry() (string, []*Book, error) {
	filePaths, err := filepath.Glob(s.retryPath + "/*.data")
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

func (s *LocalStore) CleanupOk() error {
	fileInfo, err := ioutil.ReadDir(s.okPath)
	if err != nil {
		return err
	}
	now := time.Now()
	for _, info := range fileInfo {
		if diff := now.Sub(info.ModTime()); diff < s.keepOk {
			continue
		}
		if err = os.Remove(s.okPath + info.Name()); err != nil {
			return err
		}
	}
	return nil
}
