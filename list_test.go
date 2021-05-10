package goaio

//go test -covermode=count -v -coverprofile=coverage.out -run=TestList
//go tool cover -html=coverage.out
import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestList(t *testing.T) {
	l := &connMgr{}
	l.head.nnext = &l.head

	c1 := &AIOConn{
		aioConnBase: aioConnBase{
			fd: 1,
		},
	}

	assert.Equal(t, true, l.addIO(c1))
	assert.Equal(t, 1, c1.ioCount)
	assert.Equal(t, l.head.nnext, c1)
	assert.Equal(t, c1.nnext, &l.head)
	assert.Equal(t, c1.pprev, &l.head)

	c2 := &AIOConn{
		aioConnBase: aioConnBase{
			fd: 2,
		},
	}

	assert.Equal(t, true, l.addIO(c2))
	assert.Equal(t, 1, c2.ioCount)
	assert.Equal(t, l.head.nnext, c2)
	assert.Equal(t, c2.pprev, &l.head)
	assert.Equal(t, c2.nnext, c1)

	assert.Equal(t, true, l.addIO(c2))
	assert.Equal(t, 2, c2.ioCount)

	c3 := &AIOConn{
		aioConnBase: aioConnBase{
			fd: 3,
		},
	}

	assert.Equal(t, true, l.addIO(c3))
	assert.Equal(t, c3.nnext, c2)

	l.subIO(c2)
	assert.Equal(t, 1, c2.ioCount)

	assert.Equal(t, c3.nnext, c2)
	assert.Equal(t, c2.nnext, c1)

	l.subIO(c2)
	assert.Equal(t, c3.nnext, c1)
	assert.Equal(t, c2.nnext, c2.pprev)

	l.subIO(c1)
	assert.Equal(t, c1.nnext, c1.pprev)
	assert.Equal(t, l.head.nnext, c3)
	assert.Equal(t, c3.nnext, &l.head)
	assert.Equal(t, c3.pprev, &l.head)

	assert.Equal(t, true, l.addIO(c1))

	conns := []*AIOConn{}
	n := l.head.nnext
	for ; n != &l.head; n = n.nnext {
		conns = append(conns, n)
		fmt.Println(n.fd)
	}
	l.head.nnext = &l.head

}
