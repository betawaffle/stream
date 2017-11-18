package stream

import (
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	cacheLineSize = 64
)

type Ring struct {
	mask    int64
	mu      sync.Mutex
	readers []*Reader

	_      [64]byte
	writer unsafe.Pointer
	_      [56]byte
	rseq   int64
}

func NewRing(size int) *Ring {
	if bits.OnesCount(uint(size)) != 1 {
		panic("ring size must be a power of 2")
	}
	return &Ring{mask: int64(size) - 1}
}

func (r *Ring) Mask() int64 {
	return r.mask
}

func (r *Ring) clearWriter(old *Writer) bool {
	return atomic.CompareAndSwapPointer(&r.writer, unsafe.Pointer(old), nil)
}

func (r *Ring) minReaderSeq(max int64) int64 {
	r.mu.Lock()
	readers := r.readers
	r.mu.Unlock()

	min := max
	for _, reader := range readers {
		if seq := atomic.LoadInt64(&reader.seq); seq < min && seq != -2 {
			min = seq
		}
	}
	return min
}

func (r *Ring) swapWriter(w *Writer) *Writer {
	return (*Writer)(atomic.SwapPointer(&r.writer, unsafe.Pointer(w)))
}

func (r *Ring) waitReaders(seq int64) int64 {
	var (
		mask = r.mask
	)
	for i := 0; ; i++ {
		max := r.minReaderSeq(seq) + mask
		if seq > max {
			runtime.Gosched()
			continue
		}
		return max
	}
}

func (r *Ring) waitWriters(seq int64) int64 {
	for {
		if max := atomic.LoadInt64(&r.rseq) - 1; max >= seq {
			return max
		}
		runtime.Gosched()
	}
}
