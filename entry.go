package stream

import "github.com/betawaffle/sema"

// Entry holds a single value in a stream.
type Entry struct {
	chunk *chunk
	index int32
	sem   sema.Phore
	value interface{}
}

// Age returns the distance from this entry to the tail of the stream.
func (e *Entry) Age() int64 {
	return e.chunk.stream.Seq() - e.Seq()
}

// Get returns the value of this entry and the entry itself.
// Get is safe to call on a nil entry in which case it returns two nils.
func (e *Entry) Get() (interface{}, *Entry) {
	if e == nil {
		return nil, nil
	}
	return e.value, e
}

// Next returns the next entry with its value or waits for one to be available.
// Next will return nil at the end of the stream.
func (e *Entry) Next() (interface{}, *Entry) {
	if next := e.chunk.wait(int64(e.index) + 1); next != nil {
		return next.value, next
	}
	return nil, nil
}

// Push is just a helper to push a new value onto the same stream.
func (e *Entry) Push(value interface{}) *Entry {
	return e.chunk.stream.Push(value)
}

// Seq returns the index of this entry from the beginning of the stream.
func (e *Entry) Seq() int64 {
	return e.chunk.start + int64(e.index)
}

// TryNext returns the next entry if it can be obtained without blocking.
func (e *Entry) TryNext() *Entry {
	return e.chunk.try(int64(e.index) + 1)
}

// Value returns the value of this entry.
func (e *Entry) Value() interface{} {
	if e == nil {
		return nil
	}
	return e.value
}

// commit is called immediately after value is set.
func (e *Entry) commit() {
	e.sem.Release() // unblock the first waiter
}

// wait is called to block until the value is set.
func (e *Entry) wait() {
	e.sem.Acquire()
	e.sem.Release() // unblock the next waiter
}
