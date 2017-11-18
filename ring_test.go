package stream

import (
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

const (
	ringSize = 1 << 10
	ringMask = ringSize - 1
)

func TestRing(t *testing.T) {
	if testing.Short() {
		return
	}

	ring := NewRing(ringSize)
	data := make([]int64, ringSize)
	mask := ring.Mask()

	w := ring.NewWriter()
	n := runtime.GOMAXPROCS(0)

	done := sync.WaitGroup{}
	done.Add(n)

	for g := 0; g < n; g++ {
		go func(r *Reader) {
			defer done.Done()
			for i := 0; i < ringSize; i++ {
				seq := r.Next()
				if seq < 0 {
					t.Errorf("reader initialized too late")
					break
				}
				if val := data[seq&mask]; val != seq {
					t.Errorf("value at %d should be %d, got %d", i, seq, val)
				}
			}
		}(ring.NewReader())
	}

	for i := 0; i < ringSize; i++ {
		seq := w.Reserve(1)
		data[seq&mask] = seq
		w.Commit()
	}
	done.Wait()
}

func BenchmarkRingReader(b *testing.B) {
	ring := NewRing(ringSize)
	data := make([]int64, ringSize)
	mask := ring.Mask()

	var (
		rs = makeReaders(ring)
		gs int32
	)

	go func(n int) {
		w := ring.NewWriter()

		for i := 0; i < n; i++ {
			seq := w.Reserve(1)
			data[seq&mask] = seq
			w.Commit()
		}
	}(b.N)

	b.RunParallel(func(pb *testing.PB) {
		g := atomic.AddInt32(&gs, 1) - 1
		r := rs[g]
		defer r.Close()

		for pb.Next() {
			seq := r.Next()
			if seq < 0 {
				b.Errorf("negative sequence")
				return
			}
			if val := data[seq&mask]; val != seq {
				b.Fatalf("race at %d, saw %d", seq, val)
			}
		}
	})
}

func BenchmarkRingWriter(b *testing.B) {
	log.SetFlags(log.Lmicroseconds)

	ring := NewRing(ringSize)
	data := make([]int64, ringSize)
	mask := ring.Mask()

	b.RunParallel(func(pb *testing.PB) {
		w := ring.NewWriter()

		for pb.Next() {
			seq := w.Reserve(1)
			data[seq&mask] = seq
			w.Commit()
		}
	})
}

func makeReaders(ring *Ring) []*Reader {
	readers := make([]*Reader, runtime.GOMAXPROCS(0))
	for i := range readers {
		readers[i] = ring.NewReader()
	}
	return readers
}
