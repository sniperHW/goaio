package goaio

import (
	"time"
)

const (
	waitting int32 = 0
	firing   int32 = 1
	stoped   int32 = 2
)

type timer struct {
	idx       int
	timeoutCB func()
	deadline  time.Time
	status    int32
}

type timedHeap []*timer

func (h timedHeap) Len() int { return len(h) }

func (h timedHeap) Less(i, j int) bool { return h[i].deadline.Before(h[j].deadline) }

func (h timedHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idx = i
	h[j].idx = j
}

func (h *timedHeap) Push(x interface{}) {
	*h = append(*h, x.(*timer))
	n := len(*h)
	(*h)[n-1].idx = n - 1
}

func (h *timedHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // avoid memory leak
	*h = old[0 : n-1]
	return x
}
