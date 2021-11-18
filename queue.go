package goque

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"os"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
)

// Queue is a standard FIFO (first in, first out) queue.
type Queue struct {
	mu      sync.RWMutex
	cond    *sync.Cond
	DataDir string
	db      *leveldb.DB
	head    uint64
	tail    uint64
	isOpen  bool
}

// OpenQueue opens a queue if one exists at the given directory. If one
// does not already exist, a new queue is created.
func OpenQueue(dataDir string) (*Queue, error) {
	var err error

	// Create a new Queue.
	q := &Queue{
		DataDir: dataDir,
		db:      &leveldb.DB{},
		head:    0,
		tail:    0,
		isOpen:  false,
	}

	q.cond = sync.NewCond(&q.mu)

	// Open database for the queue.
	q.db, err = leveldb.OpenFile(dataDir, nil)
	if err != nil {
		return q, err
	}

	// Check if this Goque type can open the requested data directory.
	ok, err := checkGoqueType(dataDir, goqueQueue)
	if err != nil {
		return q, err
	}
	if !ok {
		return q, ErrIncompatibleType
	}

	// Set isOpen and return.
	q.isOpen = true
	return q, q.init()
}

// Enqueue adds an item to the queue.
func (q *Queue) Enqueue(value []byte) (*Item, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.enqueue(value)
}

// EnqueueString is a helper function for Enqueue that accepts a
// value as a string rather than a byte slice.
func (q *Queue) EnqueueString(value string) (*Item, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.enqueue([]byte(value))
}

// EnqueueObject is a helper function for Enqueue that accepts any
// value type, which is then encoded into a byte slice using
// encoding/gob.
//
// Objects containing pointers with zero values will decode to nil
// when using this function. This is due to how the encoding/gob
// package works. Because of this, you should only use this function
// to encode simple types.
func (q *Queue) EnqueueObject(value interface{}) (*Item, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	return q.enqueue(buffer.Bytes())
}

// EnqueueObjectAsJSON is a helper function for Enqueue that accepts
// any value type, which is then encoded into a JSON byte slice using
// encoding/json.
//
// Use this function to handle encoding of complex types.
func (q *Queue) EnqueueObjectAsJSON(value interface{}) (*Item, error) {
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	return q.enqueue(jsonBytes)
}

// Dequeue removes the next item in the queue and returns it.
func (q *Queue) Dequeue() (*Item, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.dequeue()
}

func (q *Queue) DequeueBlock() (*Item, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		item, err := q.dequeue()
		if err == ErrEmpty {
			q.cond.Wait()
			continue
		} else if err != nil {
			return nil, err
		}

		return item, nil
	}
}

func (q *Queue) dequeue() (*Item, error) {
	// Check if queue is closed.
	if !q.isOpen {
		return nil, ErrDBClosed
	}

	// Try to get the next item in the queue.
	item, err := q.getItemByID(q.head + 1)
	if err != nil {
		return nil, err
	}

	// Remove this item from the queue.
	if err := q.db.Delete(item.Key, nil); err != nil {
		return nil, err
	}

	// Increment head position.
	q.head++

	return item, nil
}

func (q *Queue) enqueue(value []byte) (*Item, error) {
	// Check if queue is closed.
	if !q.isOpen {
		return nil, ErrDBClosed
	}

	// Create new Item.
	item := &Item{
		ID:    q.tail + 1,
		Key:   idToKey(q.tail + 1),
		Value: value,
	}

	// Add it to the queue.
	if err := q.db.Put(item.Key, item.Value, nil); err != nil {
		return nil, err
	}

	// Increment tail position.
	q.tail++

	// Wake-up all goroutines waiting for items in queue
	q.cond.Broadcast()

	return item, nil
}

// Peek returns the next item in the queue without removing it.
func (q *Queue) Peek() (*Item, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	// Check if queue is closed.
	if !q.isOpen {
		return nil, ErrDBClosed
	}

	return q.getItemByID(q.head + 1)
}

func (q *Queue) PeekBlock() (*Item, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check if queue is closed.
	if !q.isOpen {
		return nil, ErrDBClosed
	}

	for {
		item, err := q.getItemByID(q.head + 1)
		if err == ErrEmpty {
			q.cond.Wait()
			continue
		} else if err != nil {
			return nil, err
		}

		return item, nil
	}
}

// PeekByOffset returns the item located at the given offset,
// starting from the head of the queue, without removing it.
func (q *Queue) PeekByOffset(offset uint64) (*Item, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	// Check if queue is closed.
	if !q.isOpen {
		return nil, ErrDBClosed
	}

	return q.getItemByID(q.head + offset + 1)
}

// PeekByID returns the item with the given ID without removing it.
func (q *Queue) PeekByID(id uint64) (*Item, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	// Check if queue is closed.
	if !q.isOpen {
		return nil, ErrDBClosed
	}

	return q.getItemByID(id)
}

// Update updates an item in the queue without changing its position.
func (q *Queue) Update(id uint64, newValue []byte) (*Item, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check if queue is closed.
	if !q.isOpen {
		return nil, ErrDBClosed
	}

	// Check if item exists in queue.
	if id <= q.head || id > q.tail {
		return nil, ErrOutOfBounds
	}

	// Create new Item.
	item := &Item{
		ID:    id,
		Key:   idToKey(id),
		Value: newValue,
	}

	// Update this item in the queue.
	if err := q.db.Put(item.Key, item.Value, nil); err != nil {
		return nil, err
	}

	return item, nil
}

// UpdateString is a helper function for Update that accepts a value
// as a string rather than a byte slice.
func (q *Queue) UpdateString(id uint64, newValue string) (*Item, error) {
	return q.Update(id, []byte(newValue))
}

// UpdateObject is a helper function for Update that accepts any
// value type, which is then encoded into a byte slice using
// encoding/gob.
//
// Objects containing pointers with zero values will decode to nil
// when using this function. This is due to how the encoding/gob
// package works. Because of this, you should only use this function
// to encode simple types.
func (q *Queue) UpdateObject(id uint64, newValue interface{}) (*Item, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	if err := enc.Encode(newValue); err != nil {
		return nil, err
	}
	return q.Update(id, buffer.Bytes())
}

// UpdateObjectAsJSON is a helper function for Update that accepts
// any value type, which is then encoded into a JSON byte slice using
// encoding/json.
//
// Use this function to handle encoding of complex types.
func (q *Queue) UpdateObjectAsJSON(id uint64, newValue interface{}) (*Item, error) {
	jsonBytes, err := json.Marshal(newValue)
	if err != nil {
		return nil, err
	}

	return q.Update(id, jsonBytes)
}

// Length returns the total number of items in the queue.
func (q *Queue) Length() uint64 {
	return q.tail - q.head
}

// Close closes the LevelDB database of the queue.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check if queue is already closed.
	if !q.isOpen {
		return nil
	}

	// Close the LevelDB database.
	if err := q.db.Close(); err != nil {
		return err
	}

	// Reset queue head and tail and set
	// isOpen to false.
	q.head = 0
	q.tail = 0
	q.isOpen = false

	// Wake-up any waiting goroutines for blocking queue access - they should get a ErrQueueClosed
	q.cond.Broadcast()

	return nil
}

// Drop closes and deletes the LevelDB database of the queue.
func (q *Queue) Drop() error {
	if err := q.Close(); err != nil {
		return err
	}

	return os.RemoveAll(q.DataDir)
}

// getItemByID returns an item, if found, for the given ID.
func (q *Queue) getItemByID(id uint64) (*Item, error) {
	// Check if empty or out of bounds.
	if q.Length() == 0 {
		return nil, ErrEmpty
	} else if id <= q.head || id > q.tail {
		return nil, ErrOutOfBounds
	}

	// Get item from database.
	var err error
	item := &Item{ID: id, Key: idToKey(id)}
	if item.Value, err = q.db.Get(item.Key, nil); err != nil {
		return nil, err
	}

	return item, nil
}

// init initializes the queue data.
func (q *Queue) init() error {
	// Create a new LevelDB Iterator.
	iter := q.db.NewIterator(nil, nil)
	defer iter.Release()

	// Set queue head to the first item.
	if iter.First() {
		q.head = keyToID(iter.Key()) - 1
	}

	// Set queue tail to the last item.
	if iter.Last() {
		q.tail = keyToID(iter.Key())
	}

	return iter.Error()
}
