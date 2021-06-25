package goaio

import (
	//"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestList(t *testing.T) {
	l := linkList{}

	for i := 0; i < 4097; i++ {
		l.push(i)
	}

	for i := 0; i < 4097; i++ {
		l.pop()
	}

	assert.Equal(t, 4096, l.poolCount)

	l.push(1)

	assert.Equal(t, 4095, l.poolCount)

}
