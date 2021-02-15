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

	var r *AIOConn

	r, _ = q.pop()
	assert.Equal(t, r.userData.(int), 1)

	r, _ = q.pop()
	assert.Equal(t, r.userData.(int), 2)

	r, _ = q.pop()
	assert.Equal(t, r.userData.(int), 3)

	r, _ = q.pop()
	assert.Equal(t, r.userData.(int), 4)

}
