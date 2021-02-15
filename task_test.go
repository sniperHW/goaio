package goaio

//go test -covermode=count -v -coverprofile=coverage.out -run=TestTaskQueue
//go tool cover -html=coverage.out
import (
	//"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTaskQueue(t *testing.T) {
	q := NewTaskQueue()
	q.push(&AIOConn{
		userData: 1,
	})
	q.push(&AIOConn{
		userData: 2,
	})
	q.push(&AIOConn{
		userData: 3,
	})
	q.push(&AIOConn{
		userData: 4,
	})

	head, _ := q.pop()
	i := 1
	for head != nil {
		assert.Equal(t, head.userData.(int), i)
		head = head.nnext
		i++
	}

	q.push(&AIOConn{
		userData: 5,
	})
	q.push(&AIOConn{
		userData: 6,
	})
	q.push(&AIOConn{
		userData: 7,
	})
	q.push(&AIOConn{
		userData: 8,
	})

	head, _ = q.pop()
	i = 5
	for head != nil {
		assert.Equal(t, head.userData.(int), i)
		head = head.nnext
		i++
	}

	/*var r *AIOConn

	r, _ = q.pop()
	assert.Equal(t, r.userData.(int), 1)

	r, _ = q.pop()
	assert.Equal(t, r.userData.(int), 2)

	r, _ = q.pop()
	assert.Equal(t, r.userData.(int), 3)

	r, _ = q.pop()
	assert.Equal(t, r.userData.(int), 4)*/

}
