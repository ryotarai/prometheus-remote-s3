package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Buffer struct {
	bufferDir  string
	bufferPath string
	writer     io.WriteCloser
	mutex      sync.Mutex
}

func NewBuffer(bufferDir string) *Buffer {
	return &Buffer{
		bufferDir:  bufferDir,
		bufferPath: filepath.Join(bufferDir, "current.jsonl"),
		mutex:      sync.Mutex{},
	}
}

type line struct {
	Timestamp  time.Time         `json:"timestamp"`
	MetricName string            `json:"metricName"`
	IsNaN      bool              `json:"isNaN"`
	Value      float64           `json:"value"`
	Labels     map[string]string `json:"labels"`
}

func (b *Buffer) Put(t time.Time, name string, value float64, labels map[string]string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.writer == nil {
		_, err := os.Stat(b.bufferDir)
		if os.IsNotExist(err) {
			log.Printf("Creating a buffer directory because it does not exist")
			err := os.Mkdir(b.bufferDir, 0700)
			if err != nil {
				return err
			}
		}

		log.Printf("Opening %s", b.bufferPath)
		f, err := os.OpenFile(b.bufferPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return err
		}
		b.writer = f
	}

	e := json.NewEncoder(b.writer)
	l := line{
		Timestamp:  t,
		MetricName: name,
		IsNaN:      false,
		Value:      value,
		Labels:     labels,
	}
	if math.IsNaN(l.Value) {
		l.Value = 0
		l.IsNaN = true
	}
	err := e.Encode(l)
	if err != nil {
		return err
	}

	return nil
}

func (b *Buffer) Rotate() (string, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.writer == nil {
		return "", errors.New("no record is buffered")
	}

	err := b.writer.Close()
	if err != nil {
		return "", err
	}
	b.writer = nil

	path := filepath.Join(b.bufferDir, fmt.Sprintf("%d.done.jsonl", time.Now().UnixNano()))
	err = os.Rename(b.bufferPath, path)
	if err != nil {
		return "", err
	}

	return path, nil
}
