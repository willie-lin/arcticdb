package arcticdb

import (
	"encoding/binary"

	"github.com/tidwall/wal"
)

type WAL struct {
	log *wal.Log

	firstIndex uint64
	lastIndex  uint64
}

func OpenWAL(path string) (*WAL, error) {
	log, err := wal.Open(path+".v1", nil)
	if err != nil {
		return nil, err
	}

	firstIndex, err := log.FirstIndex()
	if err != nil {
		return nil, err
	}

	lastIndex, err := log.LastIndex()
	if err != nil {
		return nil, err
	}

	return &WAL{
		log:        log,
		firstIndex: firstIndex,
		lastIndex:  lastIndex,
	}, nil
}

func (w *WAL) FirstIndex() uint64 {
	return w.firstIndex
}

func (w *WAL) LastIndex() uint64 {
	return w.lastIndex
}

func (w *WAL) Close() error {
	return w.log.Close()
}

func (w *WAL) LogTableCreate(tableName string) error {
	return w.log.Write(0, []byte(tableName))
}

func (w *WAL) LogWrite(tx uint64, tableName string, value []byte) error {
	tableNameLength := uint16(len(tableName))
	msg := make(
		[]byte,
		// length of tableName (uint16), tableName, value
		2+int(tableNameLength)+len(value),
	)
	binary.LittleEndian.PutUint16(msg, len(tableName))
	copy(msg[2:], tableName)
	copy(msg[2+len(tableName):], value)
	return w.log.Write(tx, msg)
}

type WALReplayer interface {
	ReplayWrite(tx uint64, tableName string, value []byte) error
	ReplayTableCreate(tableName string, config *TableConfig) error
}

func (w *WAL) Replay(f func(tx uint64, tableName string, value []byte) error) error {
	if w.firstIndex == 0 {
		// Empty WAL
		return nil
	}

	for i := w.firstIndex; i <= w.lastIndex; i++ {
		value, err := w.log.Read(i)
		if err != nil {
			return err
		}

		tableNameLength := binary.LittleEndian.Uint16(value)
		tableName := string(value[2 : 2+tableNameLength])
		value = value[2+tableNameLength:]

		err = f(i, tableName, value)
		if err != nil {
			return err
		}
	}

	return nil
}
