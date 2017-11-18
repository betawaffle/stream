package stream

import (
	"sync/atomic"

	"github.com/betawaffle/sema"
)

type Writer struct {
	ring *Ring
	prev *Writer
	seq  int64
	end  int64

	next    int64      //     read by next writer
	ready   sema.Phore // released by next writer
	reserve sema.Phore // acquired by next writer
	commit  sema.Phore // acquired by next writer
}

func (r *Ring) NewWriter() *Writer {
	w := &Writer{
		ring: r,
		seq:  -1,
		end:  -1,
	}
	w.ready.SetCount(1)
	return w
}

const spin = false

func (w *Writer) Commit() {
	// Wait for the previous writer to finish
	if w.prev != nil {
		w.prev.commit.Acquire() // slow
		w.prev.ready.ReleaseHandoff()
		w.prev = nil
	}

	// Do the actual commit
	atomic.StoreInt64(&w.ring.rseq, w.next)

	// Unblock future writers
	if w.ring.clearWriter(w) {
		w.reserve.SetCount(0) // since we released it ourselves
		w.ready.SetCount(1)
	} else {
		w.commit.ReleaseHandoff() // another writer has seen us
	}
}

func (w *Writer) Reserve(n int) (seq int64) {
	if int64(n) > w.ring.mask {
		panic("reservation too large")
	}

	// Wait for an observing writer to read w.next
	w.ready.Acquire()

	// Announce ourselves and calculate the next slot
	prev := w.ring.swapWriter(w)
	if prev == nil {
		seq = w.ring.rseq
	} else {
		prev.reserve.AcquireSpin()
		seq = prev.next
	}
	next := seq + int64(n)

	// Unlock the writer waiting on w.next
	w.next = next
	w.reserve.ReleaseHandoff()

	// Wait for readers to finish with the slot(s) we need
	if next > w.end {
		w.end = w.ring.waitReaders(next)
	}
	w.seq, w.prev = seq, prev
	return
}
