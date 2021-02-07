package goaio

//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out
import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type task struct {
	counter *int
}

func (this *task) Do() {
	(*this.counter)++
	fmt.Println(*this.counter)
}

type task2 struct {
}

func (this *task2) Do() {
	fmt.Println("task2")
}

func TestTask(t *testing.T) {
	{
		counter := 0
		q := NewTaskQueue()

		q.push(&task{counter: &counter})
		q.push(&task{counter: &counter})
		q.push(&task{counter: &counter})

		assert.Equal(t, 3, q.l.Len())

		q.close()

		var err error
		var v TaskI

		for {
			v, err = q.pop()
			if nil != err {
				break
			} else {
				v.Do()
			}
		}

		assert.Equal(t, 3, counter)
		assert.Equal(t, Error_TaskQueue_Closed, err)
		assert.Equal(t, Error_TaskQueue_Closed, q.push(&task{counter: &counter}))

	}

	{
		q := NewTaskQueue()
		go func() {
			v, err := q.pop()
			assert.Nil(t, err)
			v.Do()
			q.close()
		}()

		time.Sleep(time.Second)

		q.push(&task2{})

		time.Sleep(time.Second)

		_, err := q.pop()
		assert.Equal(t, Error_TaskQueue_Closed, err)
	}
}
