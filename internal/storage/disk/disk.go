// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package disk

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/grab/async"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/kelindar/talaria/internal/storage"
)

const (
	ctxTag    = "disk"
	errClosed = "unable to run commands on a closed database"
)

// Assert contract compliance
var _ storage.Storage = new(Storage)

// Storage represents disk storage.
type Storage struct {
	closed  int32           // The closed flag
	gc      async.Task      // Closing channel
	db      *badger.DB      // The underlying key-value store
	monitor monitor.Monitor // The stats client
}

// New creates a new disk-backed storage which internally uses badger KV.
func New(m monitor.Monitor) *Storage {
	return &Storage{
		monitor: m,
	}
}

// Open creates a disk storage and open the directory
func Open(dir string, name string, monitor monitor.Monitor) *Storage {
	diskStorage := New(monitor)
	tableDir := path.Join(dir, name)
	err := diskStorage.Open(tableDir)
	if err != nil {
		panic(err)
	}
	return diskStorage
}

// Open opens a directory.
func (s *Storage) Open(dir string) error {

	// Default to a /data directory
	if dir == "" {
		dir = "/data"
	}

	// Make sure we have a directory
	if err := os.MkdirAll(dir, 0777); err != nil {
		return err
	}

	// Create the options
	opts := badger.DefaultOptions(dir)
	opts.SyncWrites = false
	opts.MaxTableSize = 64 << 15
	opts.ValueLogMaxEntries = 5000
	opts.LevelOneSize = 1 << 16
	opts.LevelSizeMultiplier = 3
	opts.MaxLevels = 25
	opts.Truncate = true
	opts.Logger = &logger{s.monitor}

	// Attempt to open the database
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}

	// Setup the database and start GC
	s.db = db
	s.gc = async.Repeat(context.Background(), 1*time.Minute, s.GC)
	return nil
}

// Append adds an event into the storage.
func (s *Storage) Append(key key.Key, value []byte, ttl time.Duration) error {
	if s.isClosed() {
		return errors.New(errClosed)
	}

	if err := s.db.Update(func(tx *badger.Txn) error {
		return tx.SetEntry(&badger.Entry{
			Key:       key,
			Value:     value,
			ExpiresAt: uint64(time.Now().Add(ttl).Unix()),
		})
	}); err != nil {
		return errors.Internal("unable to append", err)
	}
	return nil
}

// Range performs a range query against the storage. It calls f sequentially for each key and value present in
// the store. If f returns false, range stops the iteration. The API is designed to be very similar to the concurrent
// map. The implementation must guarantee that the keys are lexigraphically sorted.
func (s *Storage) Range(seek, until key.Key, f func(key, value []byte) bool) error {
	if s.isClosed() {
		return errors.New(errClosed)
	}

	return s.db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			Prefix:         key.PrefixOf(seek, until),
		})
		defer it.Close()

		// Seek the prefix and check the key so we can quickly exit the iteration.
		for it.Seek(seek); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			if bytes.Compare(key, until) > 0 {
				return nil // Stop if we're reached the end
			}

			// Fetch the value
			if value, ok := s.fetch(key, item); ok && f(key, value) {
				return nil
			}
		}
		return nil
	})
}

// load attempts to load an item from either cache or badger.
func (s *Storage) fetch(key []byte, item *badger.Item) ([]byte, bool) {
	value, err := item.ValueCopy(nil)
	return value, err == nil
}

// Purge clears out the data prior to GC, to avoid some old data being
func (s *Storage) purge() (deleted, total int) {
	_ = s.db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
		})

		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			total++
			key := it.Item().Key()
			if it.Item().ExpiresAt() <= uint64(time.Now().Unix()) {
				if err := s.Delete(key); err == nil {
					deleted++
				}
			}
		}
		return nil
	})
	return
}

// Delete deletes one or multiple keys from the storage.
func (s *Storage) Delete(keys ...key.Key) error {
	const msg = "unable to delete"
	if s.isClosed() {
		return errors.New(errClosed)
	}

	txn := s.db.NewTransaction(true)
	for _, key := range keys {
		err := txn.Delete(key)

		// If the transaction is too big, commit the current transaction
		switch {
		case err == badger.ErrTxnTooBig:
			if err := txn.Commit(); err != nil {
				return errors.Internal(msg, err)
			}

			// Create a new transaction and delete
			txn = s.db.NewTransaction(true)
			if err := txn.Delete(key); err != nil {
				return errors.Internal(msg, err)
			}

		// On any other error, fail the deletion
		case err != nil:
			return errors.Internal(msg, err)

		}
	}

	// Commit the transaction
	if err := txn.Commit(); err != nil {
		return errors.Internal(msg, err)
	}
	return nil
}

// GC runs the garbage collection on the storage
func (s *Storage) GC(ctx context.Context) (interface{}, error) {
	const tag = "GC"
	const discardRatio = 0.3

	if s.gc != nil && s.gc.State() == async.IsCancelled {
		return nil, nil
	}

	deleted, total := s.purge()
	s.monitor.Gauge(ctxTag, "GC.purge", float64(deleted), "type:deleted")
	s.monitor.Gauge(ctxTag, "GC.purge", float64(total), "type:total")

	for true {
		if s.db.RunValueLogGC(discardRatio) != nil {
			s.monitor.Count1(ctxTag, "vlog.GC", "type:stopped")
			return nil, nil
		}
		s.monitor.Count1(ctxTag, "vlog.GC", "type:completed")
	}
	return nil, nil
}

// Close is used to gracefully close the connection.
func (s *Storage) Close() error {
	if s.gc != nil {
		s.gc.Cancel()
	}

	atomic.StoreInt32(&s.closed, 1)
	return s.db.Close()
}

// isClosed checks if the DB is closed or not.
func (s *Storage) isClosed() bool {
	return atomic.LoadInt32(&s.closed) == int32(1)
}

// handlePanic handles the panic and logs it out.
func handlePanic() {
	if r := recover(); r != nil {
		log.Printf("panic recovered: %ss \n %s", r, debug.Stack())
	}
}

// ------------------------------------------------------------------------------------------------------------

type logger struct {
	monitor.Monitor
}

func (l *logger) Errorf(format string, args ...interface{}) {
	l.Monitor.Error(fmt.Errorf(format, args...))
}

func (l *logger) Warningf(format string, args ...interface{}) {
	l.Monitor.Warning(fmt.Errorf(format, args...))
}

func (l *logger) Infof(format string, args ...interface{}) {
}

func (l *logger) Debugf(format string, args ...interface{}) {
}
