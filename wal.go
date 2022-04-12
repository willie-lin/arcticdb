package arcticdb

import (
	"github.com/tidwall/wal"
)

type WAL struct {
	log *wal.Log
}

func OpenWAL(path string) (*WAL, error) {
	log, err := wal.Open(path, nil)
	if err != nil {
		return nil, err
	}
	return &WAL{log: log}, nil
}

func (w *WAL) Close() error {
	return w.log.Close()
}

func (w *WAL) Write(tx uint64, value []byte) error {
	return w.log.Write(tx, value)
}
