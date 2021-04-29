package goaio

//go test -covermode=count -v -coverprofile=coverage.out -run=TestCompleteQueue
//go tool cover -html=coverage.out
import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCompleteQueue(t *testing.T) {
	que := newCompletetionQueue()
	for i := 0; i < 1024; i++ {
		que.push(&AIOResult{
			Bytestransfer: i,
		})
	}
	assert.Equal(t, false, que.empty())

	que.push(&AIOResult{
		Bytestransfer: 1024,
	})

	que.push(&AIOResult{
		Bytestransfer: 1025,
	})

	for i := 0; i <= 1025; i++ {
		r := que.pop()
		assert.Equal(t, i, r.Bytestransfer)
	}

	assert.Equal(t, true, que.empty())
}
