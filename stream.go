package stream

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// Stream represents a possibly unbounded sequence of interface values. It is
// designed for fast concurrent reads and writes. Readers maintain independent
// cursors into the stream. Entries can be garbage collected once all readers
// move beyond them.
type Stream struct {
	tail unsafe.Pointer
	wseq int64
	mu   sync.Mutex
	done bool
}

// Done marks the stream as complete, unblocking any waiters. Any entries
// pushed concurrently with or after this call may or may not be seen.
func (s *Stream) Done() {
	s.mu.Lock()
	if s.done {
		s.mu.Unlock()
		return
	}
	tail := (*chunk)(s.tail)
	s.done = true
	s.mu.Unlock()
	tail.done()
}

// Head waits for the first entry in the current chunk.
func (s *Stream) Head() *Entry {
	return s.initTail().wait(0)
}

// Len returns the number of entries pushed to the stream.
func (s *Stream) Len() int {
	return int(atomic.LoadInt64(&s.wseq))
}

// Next waits for the next entry to be pushed and returns it.
func (s *Stream) Next() *Entry {
	c := s.initTail()
	i := atomic.LoadInt64(&s.wseq)
	return c.wait(i)
}

// Push appends a new entry to the stream. It may or may not return an entry
// after Done has been called.
func (s *Stream) Push(value interface{}) *Entry {
	tail := s.initTail()
	if tail == nil {
		return nil
	}
	i := atomic.AddInt64(&s.wseq, 1) - 1 - tail.start
	e := tail.entry(i)
	e.value = value
	e.commit()
	return e
}

// Seq returns the highest entry index used by the stream.
// Seq will return -1 if the stream is empty.
func (s *Stream) Seq() int64 {
	return atomic.LoadInt64(&s.wseq) - 1
}

// initTail ensures there is a chunk allocated and pointed to by tail.
func (s *Stream) initTail() *chunk {
	if tail := (*chunk)(atomic.LoadPointer(&s.tail)); tail != nil {
		return tail
	}
	s.mu.Lock()
	if tail := (*chunk)(s.tail); tail != nil || s.done {
		s.mu.Unlock()
		return tail
	}
	tail := &chunk{
		stream: s,
	}
	atomic.StorePointer(&s.tail, unsafe.Pointer(tail))
	s.mu.Unlock()
	return tail
}

// makeNext ensures that the passed chunk has a next chunk and returns it.
func (s *Stream) makeNext(prev *chunk) *chunk {
	s.mu.Lock()
	next := (*chunk)(prev.next)
	if next == nil && !s.done {
		next = &chunk{
			stream: s,
			start:  prev.start + chunkSize,
		}
		prev.setNext(next)
		atomic.StorePointer(&s.tail, unsafe.Pointer(next))
	}
	s.mu.Unlock()
	return next
}
