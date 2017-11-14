package stream

import (
	"sync/atomic"
	"unsafe"

	"github.com/betawaffle/sema"
)

const (
	chunkSize = 2 * 64 * 8 // 32,768 bytes
)

type chunk struct {
	stream  *Stream
	start   int64
	next    unsafe.Pointer
	sem     sema.Phore
	entries [chunkSize]Entry
}

// done marks this chunk as the last chunk, waking anybody waiting for the next
// chunk to arrive.
func (c *chunk) done() {
	c.sem.Release() // unblock the first waiter
}

// entry returns the entry at the given index. The provided index is relative
// to this chunk, not the beginning of the stream.
func (c *chunk) entry(i int64) *Entry {
	for {
		if i < chunkSize {
			e := &c.entries[i]
			e.chunk = c
			e.index = int32(i)
			return e
		}
		c, i = c.makeNext(), i-chunkSize
	}
}

// makeNext ensures there is a chunk in the next pointer and returns it.
func (c *chunk) makeNext() *chunk {
	if next := (*chunk)(atomic.LoadPointer(&c.next)); next != nil {
		return next
	}
	return c.stream.makeNext(c)
}

// try returns the entry at the given index without blocking. If the entry
// isn't ready, try will return nil.
func (c *chunk) try(i int64) *Entry {
	for {
		if i < chunkSize {
			if e := &c.entries[i]; e.sem.Count() > 0 {
				return e
			}
		} else if next := (*chunk)(atomic.LoadPointer(&c.next)); next != nil {
			c, i = next, i-chunkSize
			continue
		}
		return nil
	}
}

// setNext sets the passed chunk as the next one in the stream.
func (c *chunk) setNext(next *chunk) {
	atomic.StorePointer(&c.next, unsafe.Pointer(next))
	c.sem.Release() // unblock the first waiter
}

// wait blocks until the entry at the given index is ready. The provided index
// is relative to this chunk, not the beginning of the stream.
func (c *chunk) wait(i int64) *Entry {
	for {
		if i < chunkSize {
			e := &c.entries[i]
			e.wait()
			return e
		}
		c, i = c.waitNext(), i-chunkSize
	}
}

// waitNext blocks until the next chunk is available.
func (c *chunk) waitNext() *chunk {
	if next := (*chunk)(atomic.LoadPointer(&c.next)); next != nil {
		return next
	}
	c.sem.Acquire()
	c.sem.Release() // unblock the next waiter
	return (*chunk)(c.next)
}
