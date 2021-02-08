package goaio

//go test -covermode=count -v -coverprofile=coverage.out -run=TestRingQueue
//go tool cover -html=coverage.out
import (
	//"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRingQueue(t *testing.T) {
	q := newAioContextQueue(5)
	assert.Equal(t, 6, len(q.queue))
	assert.Equal(t, true, q.empty())

	q.add(aioContext{context: 1})
	q.add(aioContext{context: 2})
	q.add(aioContext{context: 3})
	q.add(aioContext{context: 4})
	q.add(aioContext{context: 5})

	assert.Equal(t, ErrBusy, q.add(aioContext{context: 6}))

	assert.Equal(t, 1, q.front().context.(int))
	q.popFront()

	assert.Equal(t, nil, q.add(aioContext{context: 6}))

	assert.Equal(t, 2, q.front().context.(int))
	q.popFront()

	assert.Equal(t, 3, q.front().context.(int))
	q.popFront()

	assert.Equal(t, 4, q.front().context.(int))
	q.popFront()

	assert.Equal(t, 5, q.front().context.(int))
	q.popFront()

	assert.Equal(t, 6, q.front().context.(int))
	q.popFront()

	assert.Equal(t, true, q.empty())

	q.add(aioContext{context: 1})
	q.add(aioContext{context: 2})
	q.add(aioContext{context: 3})
	q.add(aioContext{context: 4})
	q.add(aioContext{context: 5})

	f := q.front()
	f.context = 11

	f = q.front()
	assert.Equal(t, 11, f.context.(int))

	f.context = 1

	assert.Equal(t, 1, q.front().context.(int))
	q.popFront()

	assert.Equal(t, 2, q.front().context.(int))
	q.popFront()

	assert.Equal(t, 3, q.front().context.(int))
	q.popFront()

	assert.Equal(t, 4, q.front().context.(int))
	q.popFront()

	assert.Equal(t, 5, q.front().context.(int))
	q.popFront()

}
