package wrap

import (
	"github.com/sniperHW/goaio"
	"github.com/sniperHW/goaio/example/wrap/gopool"
	"math/rand"
	"net"
	"runtime"
	"sync"
)

type bufferPool struct {
	pool sync.Pool
}

const PoolBuffSize uint64 = 1024 * 1024 * 2

var buffPool *bufferPool = newBufferPool()

var aioService *SocketService = NewSocketService(ServiceOption{
	PollerCount:              1,
	WorkerPerPoller:          runtime.NumCPU(),
	CompleteRoutinePerPoller: 4,
})

func newBufferPool() *bufferPool {
	return &bufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, PoolBuffSize)
			},
		},
	}
}

func (p *bufferPool) Acquire() []byte {
	return p.pool.Get().([]byte)
}

func (p *bufferPool) Release(buff []byte) {
	if uint64(cap(buff)) == PoolBuffSize {
		p.pool.Put(buff[:cap(buff)])
	}
}

func GetBuffPool() *bufferPool {
	return buffPool
}

var sendRoutinePool *gopool.Pool = gopool.New(gopool.Option{
	MaxRoutineCount: 1024,
	Mode:            gopool.QueueMode,
})

type SocketService struct {
	services []*goaio.AIOService
}

func (this *SocketService) completeRoutine(s *goaio.AIOService) {
	for {
		res, ok := s.GetCompleteStatus()
		if !ok {
			break
		} else {
			context := res.Context.(*ioContext)
			if context.t == 'r' {
				context.s.onRecvComplete(&res)
			} else {
				context.s.onSendComplete(&res)
			}
		}
	}
}

func (this *SocketService) createAIOConn(conn net.Conn) (*goaio.AIOConn, error) {
	idx := rand.Int() % len(this.services)
	c, err := this.services[idx].CreateAIOConn(conn, goaio.AIOConnOption{
		SendqueSize: 1,
		RecvqueSize: 1,
		ShareBuff:   GetBuffPool(),
	})
	return c, err
}

func (this *SocketService) Close() {
	for i, _ := range this.services {
		this.services[i].Close()
	}
}

type ServiceOption struct {
	PollerCount              int
	WorkerPerPoller          int
	CompleteRoutinePerPoller int
}

func NewSocketService(o ServiceOption) *SocketService {
	s := &SocketService{}

	if o.PollerCount == 0 {
		o.PollerCount = 1
	}

	if o.WorkerPerPoller == 0 {
		o.WorkerPerPoller = 1
	}

	if o.CompleteRoutinePerPoller == 0 {
		o.CompleteRoutinePerPoller = 1
	}

	for i := 0; i < o.PollerCount; i++ {
		se := goaio.NewAIOService(o.WorkerPerPoller)
		s.services = append(s.services, se)
		for j := 0; j < o.CompleteRoutinePerPoller; j++ {
			go s.completeRoutine(se)
		}
	}

	return s
}
