package wrap

import (
	"container/list"
	"errors"
	"github.com/sniperHW/goaio"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var (
	Error_TaskQueue_Closed = errors.New("task queue closed")
)

type taskQueue struct {
	mu        sync.Mutex
	cond      *sync.Cond
	tail      *Socket
	closed    bool
	closeOnce sync.Once
	waitCount int
}

func NewTaskQueue() *taskQueue {
	q := &taskQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (this *taskQueue) close() {
	this.closeOnce.Do(func() {
		this.mu.Lock()
		this.closed = true
		this.mu.Unlock()
		this.cond.Broadcast()
	})
}

func (this *taskQueue) push(t *Socket) error {
	this.mu.Lock()
	if this.closed {
		this.mu.Unlock()
		return Error_TaskQueue_Closed
	}

	var head *Socket
	if this.tail == nil {
		head = t
	} else {
		head = this.tail.nnext
		this.tail.nnext = t
	}
	t.nnext = head
	this.tail = t

	waitCount := this.waitCount
	this.mu.Unlock()

	if waitCount > 0 {
		this.cond.Signal()
	}

	return nil
}

func (this *taskQueue) pop() (*Socket, error) {
	this.mu.Lock()
	for this.tail == nil {
		if this.closed {
			this.mu.Unlock()
			return nil, Error_TaskQueue_Closed
		} else {
			this.waitCount++
			this.cond.Wait()
			this.waitCount--
		}
	}

	head := this.tail.nnext
	this.tail.nnext = nil
	this.tail = nil

	this.mu.Unlock()

	return head, nil
}

type Message interface {
	Bytes() []byte
}

type EnCoder interface {
	EnCode(o interface{}) (Message, error)
}

type SocketSerice struct {
	services          []*goaio.AIOService
	outboundTaskQueue []*taskQueue
	shareBuffer       goaio.ShareBuffer
}

func (this *SocketSerice) completeRoutine(s *goaio.AIOService) {
	for {
		res, err := s.GetCompleteStatus()
		if nil != err {
			break
		} else {
			c := res.Conn.GetUserData().(*Socket)
			if res.Context.(rune) == 'r' {
				c.onRecvComplete(&res)
			} else {
				c.onSendComplete(&res)
			}
		}
	}
}

func (this *SocketSerice) Bind(conn net.Conn, ud interface{}) (*goaio.AIOConn, *taskQueue, error) {
	idx := rand.Int() % len(this.services)
	c, err := this.services[idx].Bind(conn, goaio.AIOConnOption{
		SendqueSize: 1,
		RecvqueSize: 1,
		ShareBuff:   this.shareBuffer,
		UserData:    ud,
	})
	return c, this.outboundTaskQueue[idx], err
}

func (this *SocketSerice) outboundRoutine(tq *taskQueue) {
	for {
		head, err := tq.pop()
		if nil != err {
			return
		} else {
			for head != nil {
				next := head.nnext
				head.doSend()
				head = next
			}
		}
	}
}

func (this *SocketSerice) Close() {
	for i, _ := range this.services {
		this.services[i].Close()
		this.outboundTaskQueue[i].close()
	}
}

func NewSocketSerice(shareBuffer goaio.ShareBuffer) *SocketSerice {
	s := &SocketSerice{
		shareBuffer: shareBuffer,
	}

	for i := 0; i < 2; i++ {
		se := goaio.NewAIOService(2)
		tq := NewTaskQueue()
		s.services = append(s.services, se)
		s.outboundTaskQueue = append(s.outboundTaskQueue, tq)
		go s.completeRoutine(se)
		go s.outboundRoutine(tq)
	}

	return s
}

type InBoundProcessor interface {
	GetRecvBuff() []byte
	OnData([]byte)
	Unpack() (interface{}, error)
	OnSocketClose()
}

type defaultInBoundProcessor struct {
	bytes  int
	buffer []byte
}

func (this *defaultInBoundProcessor) GetRecvBuff() []byte {
	return this.buffer
}

func (this *defaultInBoundProcessor) Unpack() (interface{}, error) {
	if 0 == this.bytes {
		return nil, nil
	} else {
		msg := make([]byte, this.bytes)
		copy(msg, this.buffer[:this.bytes])
		this.bytes = 0
		return msg, nil
	}
}

func (this *defaultInBoundProcessor) OnData(buff []byte) {
	this.bytes = len(buff)
}

func (this *defaultInBoundProcessor) OnSocketClose() {

}

const (
	fclosed  = int32(1 << 1)
	frclosed = int32(1 << 2)
)

type Socket struct {
	nnext            *Socket
	muW              sync.Mutex
	sendQueue        *list.List
	flag             int32
	aioConn          *goaio.AIOConn
	encoder          EnCoder
	inboundProcessor InBoundProcessor
	errorCallback    func(*Socket, error)
	closeCallBack    func(*Socket, error)
	inboundCallBack  func(*Socket, interface{})
	beginOnce        sync.Once
	closeOnce        sync.Once
	sendQueueSize    int
	sendLock         bool
	sendbuff         []byte
	ioWait           sync.WaitGroup
	sendOverChan     chan struct{}
	netconn          net.Conn
	tq               *taskQueue
}

func (s *Socket) setFlag(flag int32) {
	for !atomic.CompareAndSwapInt32(&s.flag, s.flag, s.flag|flag) {
	}
}

func (s *Socket) testFlag(flag int32) bool {
	return atomic.LoadInt32(&s.flag)&flag > 0
}

func (s *Socket) SetEncoder(e EnCoder) {
	s.encoder = e
}

func (s *Socket) SetSendQueueSize(size int) {
	s.muW.Lock()
	defer s.muW.Unlock()
	s.sendQueueSize = size
}

func (s *Socket) SetRecvTimeout(timeout time.Duration) {
	s.aioConn.SetRecvTimeout(timeout)
}

func (s *Socket) SetSendTimeout(timeout time.Duration) {
	s.aioConn.SetSendTimeout(timeout)
}

func (s *Socket) getDefaultInboundProcessor() InBoundProcessor {
	return &defaultInBoundProcessor{
		buffer: make([]byte, 4096),
	}
}

func (s *Socket) BeginRecv(cb func(*Socket, interface{})) (err error) {
	s.beginOnce.Do(func() {
		if nil == cb {
			panic("BeginRecv cb is nil")
		}

		if s.testFlag(fclosed | frclosed) {
			err = goaio.ErrConnClosed
		} else {
			//发起第一个recv
			if nil == s.inboundProcessor {
				s.inboundProcessor = s.getDefaultInboundProcessor()
			}
			s.inboundCallBack = cb

			s.ioWait.Add(1)
			if err = s.aioConn.Recv(s.inboundProcessor.GetRecvBuff(), 'r'); nil != err {
				s.ioWait.Done()
			}

		}
	})
	return
}

func (s *Socket) SetErrorCallBack(cb func(*Socket, error)) *Socket {
	s.errorCallback = cb
	return s
}

func (s *Socket) SetCloseCallBack(cb func(*Socket, error)) *Socket {
	s.closeCallBack = cb
	return s
}

func (s *Socket) onRecvComplete(r *goaio.AIOResult) {
	if s.testFlag(fclosed | frclosed) {
		s.ioWait.Done()
	} else {
		recvAgain := false

		defer func() {
			if !s.testFlag(fclosed|frclosed) && recvAgain {
				if nil != s.aioConn.Recv(s.inboundProcessor.GetRecvBuff(), 'r') {
					s.ioWait.Done()
				}
			} else {
				s.ioWait.Done()
			}
		}()

		if nil != r.Err {
			if nil != s.errorCallback {
				if r.Err == goaio.ErrRecvTimeout {
					recvAgain = true
				} else {
					s.Close(r.Err, 0)
				}
				s.errorCallback(s, r.Err)
			} else {
				s.Close(r.Err, 0)
			}
		} else {
			//fmt.Println("Bytestransfer", r.Bytestransfer)
			s.inboundProcessor.OnData(r.Buff[:r.Bytestransfer])
			for !s.testFlag(fclosed | frclosed) {
				msg, err := s.inboundProcessor.Unpack()
				if nil != err {
					if nil != s.errorCallback {
						s.errorCallback(s, r.Err)
					}
					s.Close(r.Err, 0)
					break
				} else if nil != msg {
					s.inboundCallBack(s, msg)
				} else {
					recvAgain = true
					break
				}
			}
		}
	}
}

func (s *Socket) emitSendTask() {
	s.ioWait.Add(1)
	if nil == s.tq.push(s) {
		s.sendLock = true
	} else {
		s.ioWait.Done()
		s.sendLock = false
	}
}

func (s *Socket) doSend() {
	s.muW.Lock()
	defer s.muW.Unlock()
	var buff []byte

	space := len(s.sendbuff)
	offset := 0
	for v := s.sendQueue.Front(); space > 0 && v != nil; v = s.sendQueue.Front() {
		b := v.Value.(Message).Bytes()
		if space >= len(b) {
			copy(s.sendbuff[offset:], b)
			offset += len(b)
			space -= len(b)
			s.sendQueue.Remove(v)
		} else {
			if offset == 0 {
				s.sendQueue.Remove(v)
				buff = b
			}
			break
		}
	}

	if offset > 0 {
		buff = s.sendbuff[:offset]
	}

	if nil != s.aioConn.Send(buff, 'w') {
		s.ioWait.Done()
	}
}

func (s *Socket) onSendComplete(r *goaio.AIOResult) {
	defer s.ioWait.Done()
	if nil == r.Err {
		s.muW.Lock()
		if s.sendQueue.Len() == 0 {
			s.sendLock = false
			if s.testFlag(fclosed) {
				close(s.sendOverChan)
			}
			s.muW.Unlock()
		} else {
			s.emitSendTask()
			s.muW.Unlock()
		}
	} else if s.testFlag(fclosed) {
		if nil != s.errorCallback {
			if r.Err != goaio.ErrSendTimeout {
				s.Close(r.Err, 0)
			}
			s.errorCallback(s, r.Err)
		} else {
			s.Close(r.Err, 0)
		}
	}
}

func (s *Socket) Send(o interface{}) error {
	if s.encoder == nil {
		panic("Send s.encoder == nil")
	} else if nil == o {
		panic("Send o == nil")
	}

	msg, err := s.encoder.EnCode(o)

	if err != nil {
		return err
	}

	return s.SendMessage(msg)

}

func (s *Socket) SendMessage(msg Message) error {
	if nil == msg {
		panic("SendMessage msg == nil")
	}

	if s.testFlag(fclosed) {
		return goaio.ErrConnClosed
	}

	s.muW.Lock()
	defer s.muW.Unlock()

	if s.sendQueue.Len() > s.sendQueueSize {
		return goaio.ErrBusy
	}

	s.sendQueue.PushBack(msg)

	if !s.sendLock {
		s.emitSendTask()
	}

	return nil
}

func (s *Socket) shutdownRead() {
	s.setFlag(frclosed)
	s.netconn.(interface{ CloseRead() error }).CloseRead()
}

func (s *Socket) Close(reason error, delay time.Duration) {
	s.closeOnce.Do(func() {
		runtime.SetFinalizer(s, nil)

		s.setFlag(fclosed)

		s.muW.Lock()
		if s.sendQueue.Len() > 0 {
			delay = delay * time.Second
			if delay <= 0 {
				s.sendQueue = list.New()
			}
		}
		s.muW.Unlock()

		if delay > 0 {
			s.shutdownRead()
			ticker := time.NewTicker(delay)
			go func() {
				select {
				case <-s.sendOverChan:
				case <-ticker.C:
				}

				ticker.Stop()
				s.aioConn.Close(nil)
			}()
		} else {
			s.aioConn.Close(nil)
		}

		go func() {
			s.ioWait.Wait()
			s.inboundProcessor.OnSocketClose()
			if nil != s.closeCallBack {
				s.closeCallBack(s, reason)
			}
		}()
	})
}

func NewSocket(service *SocketSerice, netConn net.Conn) *Socket {

	s := &Socket{}
	c, tq, err := service.Bind(netConn, s)
	if err != nil {
		return nil
	}
	s.tq = tq
	s.aioConn = c
	s.sendQueueSize = 256
	s.sendQueue = list.New()
	s.netconn = netConn
	s.sendbuff = make([]byte, 4096)
	s.sendOverChan = make(chan struct{})

	runtime.SetFinalizer(s, func(s *Socket) {
		s.Close(errors.New("gc"), 0)
	})

	return s
}
