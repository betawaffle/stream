package stream

import (
	"sync/atomic"
	"unsafe"
)

var bogusReader = new(Reader)

type Reader struct {
	ring *Ring
	next unsafe.Pointer // *Reader
	seq  int64
	end  int64
}

func (r *Ring) NewReader() *Reader {
	next := &Reader{
		ring: r,
		seq:  atomic.LoadInt64(&r.rseq) - 1,
		end:  -1,
	}
	r.mu.Lock()
	r.readers = append(r.readers, next)
	r.mu.Unlock()
	return next
}

func (r *Reader) Batch() int64 {
	return r.end
}

func (r *Reader) Close() {
	atomic.StoreInt64(&r.seq, -2)
}

func (r *Reader) Mask() int64 {
	return r.ring.Mask()
}

func (r *Reader) Next() int64 {
	seq := r.seq + 1
	if min := atomic.LoadInt64(&r.ring.rseq) - r.ring.mask; seq < min {
		atomic.StoreInt64(&r.seq, -2)
		panic("reader sequence too old")
	}
	atomic.StoreInt64(&r.seq, seq)
	if seq >= r.end {
		r.end = r.ring.waitWriters(seq)
	}
	return seq
}

func (r *Reader) loadLast() *Reader {
	if r == nil {
		return nil
	}
	for {
		if next := r.loadNext(); next != nil {
			r = next
			continue
		}
		return r
	}
}

func (r *Reader) loadNext() *Reader {
	return (*Reader)(atomic.LoadPointer(&r.next))
}
