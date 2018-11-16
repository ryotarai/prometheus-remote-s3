package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Buffer struct {
	bufferDir  string
	bufferPath string
	writer     io.WriteCloser
	mutex      sync.RWMutex
}

func NewBuffer(bufferDir string) *Buffer {
	return &Buffer{
		bufferDir:  bufferDir,
		bufferPath: filepath.Join(bufferDir, "current.ltsv"),
		mutex:      sync.RWMutex{},
	}
}

func (b *Buffer) openFile() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.writer != nil {
		return nil
	}

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

	return nil
}

func (b *Buffer) Put(timestampMilli int64, value float64, labels map[string]string) error {
	err := b.openFile()
	if err != nil {
		return err
	}

	b.mutex.RLock()
	defer b.mutex.RUnlock()

	var builder strings.Builder
	fmt.Fprintf(&builder, "timestamp:%d\tvalue:%f", timestampMilli, value)
	for k, v := range labels {
		k = strings.Replace(k, ":", "-", -1)
		fmt.Fprintf(&builder, "\t%s:%s", k, v)
	}
	fmt.Fprintln(b.writer, builder.String())

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

	path := filepath.Join(b.bufferDir, fmt.Sprintf("%d.done.ltsv", time.Now().UnixNano()))
	err = os.Rename(b.bufferPath, path)
	if err != nil {
		return "", err
	}

	return path, nil
}
