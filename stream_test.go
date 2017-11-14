package stream

import (
	"testing"
	"unsafe"
)

func TestStreamPush(t *testing.T) {
	var stream Stream
	if n := stream.Len(); n != 0 {
		t.Errorf("expected len == 0, got %d", n)
	}

	head := stream.Push(nil)
	if w := head.TryNext(); w != nil {
		t.Error("expected TryNext to return nil when nothing is available")
	} else if w.Value() != nil {
		t.Error("expected Value to return nil on a nil Cons")
	}

	x := stream.Push(1)
	y := stream.Push(2)
	z := head.Push(3)
	stream.Done()

	// if stream.Push(4) != nil {
	// 	t.Error("expected Push to fail after Done is called")
	// }
	stream.Done() // make sure it behaves nice when we call it twice

	if v := x.Value(); v != 1 {
		t.Errorf("expected 1, got %#v", v)
	}
	if v, c := head.Next(); v != 1 || c != x {
		t.Errorf("expected 1, got %#v", v)
	} else if v, c = c.Next(); v != 2 || c != y {
		t.Errorf("expected 2, got %#v", v)
	} else if v, c = c.Next(); v != 3 || c != z {
		t.Errorf("expected 3, got %#v", v)
	}
	if seq := stream.Seq(); seq != 3 {
		t.Errorf("expected seq == 3, got %d", seq)
	}
	if seq := z.Seq(); seq != 3 {
		t.Errorf("expected seq == 3, got %d", seq)
	}
	if n := stream.Len(); n != 4 {
		t.Errorf("expected len == 4, got %d", n)
	}
	if age := x.Age(); age != 2 {
		t.Errorf("expected age == 2, got %d", age)
	}
}

func BenchmarkStreamPush(b *testing.B) {
	var stream Stream

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			stream.Push(nil)
		}
	})
}

func BenchmarkStreamPushHold(b *testing.B) {
	var stream Stream
	head := stream.Push(1)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			stream.Push(nil)
		}
		if head.Value() != 1 {
			panic("foo")
		}
	})
}

func BenchmarkStreamWaitFast(b *testing.B) {
	var (
		stream Stream
		head   = stream.Push(nil)
	)
	for i := 1; i < chunkSize; i++ {
		stream.Push(nil)
	}
	head.chunk.next = unsafe.Pointer(head.chunk)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for _, next := head.Get(); pb.Next(); _, next = next.Next() {
			// ...
		}
	})
}

func BenchmarkStreamWaitSlow(b *testing.B) {
	var (
		stream Stream
		head   = stream.Push(nil)
	)
	go func(n int) {
		for i := 0; i < n; i++ {
			stream.Push(i)
		}
	}(b.N + 1)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int
		for v, next := head.Next(); pb.Next(); v, next = next.Next() {
			if v.(int) != i {
				b.Fatalf("expected %d, got %d", i, v)
			}
			i++
		}
	})
}
