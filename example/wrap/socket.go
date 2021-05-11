package wrap

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/sniperHW/goaio"
	"github.com/sniperHW/goaio/example/wrap/buffer"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrSocketClose = errors.New("socket close")
	ErrSendQueFull = errors.New("send queue full")
	ErrSendTimeout = errors.New("send timeout")
	ErrRecvTimeout = errors.New("recv timeout")
)

const (
	fclosed  = int32(1 << 1)
	frclosed = int32(1 << 2)
	fwclosed = int32(1 << 3)
	fdoclose = int32(1 << 4)
)

type ioContext struct {
	s *Socket
	t rune
}

type Encoder interface {
	EnCode(m interface{}, b *buffer.Buffer) error
}

type InBoundProcessor interface {
	GetRecvBuff() []byte
	OnData([]byte)
	Unpack() (interface{}, error)
	OnSocketClose()
}

type Socket struct {
	conn             net.Conn
	flag             int32
	ud               atomic.Value
	sendCloseChan    chan struct{}
	closeOnce        sync.Once
	beginOnce        sync.Once
	sendOnce         sync.Once
	encoder          Encoder
	inboundProcessor InBoundProcessor
	errorCallback    func(*Socket, error)
	closeCallBack    func(*Socket, error)
	inboundCallBack  func(*Socket, interface{})
	ioCount          int32
	closeReason      error
	muW              sync.Mutex
	sendQueue        *list.List
	aioConn          *goaio.AIOConn
	sendQueueSize    int
	sendLock         bool
	sendContext      ioContext
	recvContext      ioContext
	b                *buffer.Buffer
}

func (s *Socket) setFlag(flag int32) {
	for {
		f := atomic.LoadInt32(&s.flag)
		if atomic.CompareAndSwapInt32(&s.flag, f, f|flag) {
			break
		}
	}
}

func (s *Socket) testFlag(flag int32) bool {
	return atomic.LoadInt32(&s.flag)&flag > 0
}

func (s *Socket) IsClosed() bool {
	return s.testFlag(fclosed)
}

func (s *Socket) LocalAddr() net.Addr {
	return s.conn.LocalAddr()
}

func (s *Socket) RemoteAddr() net.Addr {
	return s.conn.RemoteAddr()
}

func (s *Socket) SetUserData(ud interface{}) *Socket {
	s.ud.Store(ud)
	return s
}

func (s *Socket) GetUserData() interface{} {
	return s.ud.Load()
}

func (s *Socket) GetNetConn() net.Conn {
	return s.conn
}

func (s *Socket) SetErrorCallBack(cb func(*Socket, error)) *Socket {
	s.errorCallback = cb
	return s
}

func (s *Socket) SetCloseCallBack(cb func(*Socket, error)) *Socket {
	s.closeCallBack = cb
	return s
}

func (s *Socket) SetEncoder(encoder Encoder) *Socket {
	s.encoder = encoder
	return s
}

func (s *Socket) SetInBoundProcessor(in InBoundProcessor) *Socket {
	s.inboundProcessor = in
	return s
}

func (s *Socket) ShutdownRead() {
	s.setFlag(frclosed)
	s.conn.(interface{ CloseRead() error }).CloseRead()
}

func (s *Socket) addIO() {
	atomic.AddInt32(&s.ioCount, 1)
}

func (s *Socket) ShutdownWrite() {
	s.muW.Lock()
	defer s.muW.Unlock()
	if s.testFlag(fclosed | fwclosed) {
		return
	} else {
		s.setFlag(fwclosed)
		if s.sendQueue.Len() == 0 {
			s.conn.(interface{ CloseWrite() error }).CloseWrite()
		} else {
			if !s.sendLock {
				s.emitSendTask()
			}
		}
	}
}

func (s *Socket) SetSendQueueSize(size int) *Socket {
	s.muW.Lock()
	defer s.muW.Unlock()
	s.sendQueueSize = size
	return s
}

func (s *Socket) SetRecvTimeout(timeout time.Duration) *Socket {
	s.aioConn.SetRecvTimeout(timeout)
	return s
}

func (s *Socket) SetSendTimeout(timeout time.Duration) *Socket {
	s.aioConn.SetSendTimeout(timeout)
	return s
}

func (s *Socket) onRecvComplete(r *goaio.AIOResult) {
	if s.testFlag(fclosed | frclosed) {
		s.ioDone()
	} else {
		recvAgain := false

		defer func() {
			if !s.testFlag(fclosed|frclosed) && recvAgain {
				b := s.inboundProcessor.GetRecvBuff()
				if nil != s.aioConn.Recv(&s.recvContext, b) {
					s.ioDone()
				}
			} else {
				s.ioDone()
			}
		}()

		if nil != r.Err {

			if r.Err == goaio.ErrRecvTimeout {
				r.Err = ErrRecvTimeout
				recvAgain = true
			} else {
				s.setFlag(frclosed)
			}

			if nil != s.errorCallback {
				s.errorCallback(s, r.Err)
			} else {
				s.Close(r.Err, 0)
			}

		} else {
			s.inboundProcessor.OnData(r.Buff[:r.Bytestransfer])
			for !s.testFlag(fclosed | frclosed) {
				msg, err := s.inboundProcessor.Unpack()
				if nil != err {
					s.Close(err, 0)
					if nil != s.errorCallback {
						s.errorCallback(s, err)
					}
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
	s.addIO()
	s.sendLock = true
	sendRoutinePool.Go(s.doSend)
}

func (s *Socket) doSend() {
	const maxsendsize = 65535

	s.muW.Lock()
	//只有之前请求的buff全部发送完毕才填充新的buff
	if nil == s.b {
		s.b = buffer.Get()
	}

	for v := s.sendQueue.Front(); v != nil; v = s.sendQueue.Front() {
		s.sendQueue.Remove(v)
		l := s.b.Len()
		if err := s.encoder.EnCode(v.Value, s.b); nil != err {
			//EnCode错误，这个包已经写入到b中的内容需要直接丢弃
			s.b.SetLen(l)
			fmt.Printf("encode error:%v", err)

		} else if s.b.Len() >= maxsendsize {
			break
		}
	}

	s.muW.Unlock()

	if s.b.Len() > 0 && nil == s.aioConn.Send(&s.sendContext, s.b.Bytes()) {
		return
	} else {
		s.onSendComplete(&goaio.AIOResult{})
	}

}

func (s *Socket) onSendComplete(r *goaio.AIOResult) {
	defer s.ioDone()
	if nil == r.Err {
		s.muW.Lock()
		defer s.muW.Unlock()
		//发送完成释放发送buff
		s.b.Free()
		s.b = nil
		if s.sendQueue.Len() == 0 {
			s.sendLock = false
			if s.testFlag(fclosed | fwclosed) {
				s.conn.(interface{ CloseWrite() error }).CloseWrite()
			}
		} else {
			s.emitSendTask()
			return
		}
	} else if !s.testFlag(fclosed) {
		if r.Err == goaio.ErrSendTimeout {
			r.Err = ErrSendTimeout
		}

		if nil != s.errorCallback {
			if r.Err != ErrSendTimeout {
				s.Close(r.Err, 0)
			}

			s.errorCallback(s, r.Err)

			//发送超时，但用户没有关闭socket,需要将尚未发送完成的buff再次请求发送
			if r.Err == ErrSendTimeout && !s.testFlag(fclosed) {
				s.muW.Lock()
				//timeout也可能会完成部分字节的发送
				s.b.DropFirstNBytes(r.Bytestransfer)
				s.emitSendTask()
				s.muW.Unlock()
				return
			}

		} else {
			s.Close(r.Err, 0)
		}
	}

	if s.testFlag(fclosed | fwclosed) {
		close(s.sendCloseChan)
	}
}

func (s *Socket) Send(o interface{}) error {
	if s.encoder == nil {
		return errors.New("encoder is nil")
	} else if nil == o {
		return errors.New("o is nil")
	} else {
		s.muW.Lock()
		defer s.muW.Unlock()

		if s.testFlag(fclosed | fwclosed) {
			return ErrSocketClose
		}

		if s.sendQueue.Len() > s.sendQueueSize {
			return ErrSendQueFull
		}

		s.sendQueue.PushBack(o)

		if !s.sendLock {
			s.emitSendTask()
		}

		return nil
	}

}

func (s *Socket) BeginRecv(cb func(*Socket, interface{})) (err error) {
	s.beginOnce.Do(func() {
		if nil == cb {
			err = errors.New("BeginRecv cb is nil")
			return
		}

		if nil == s.inboundProcessor {
			err = errors.New("inboundProcessor is nil")
			return
		}

		s.addIO()
		if s.testFlag(fclosed | frclosed) {
			s.ioDone()
			err = ErrSocketClose
		} else {
			//发起第一个recv
			s.inboundCallBack = cb
			if err = s.aioConn.Recv(&s.recvContext, s.inboundProcessor.GetRecvBuff()); nil != err {
				s.ioDone()
			}
		}
	})
	return
}

func (s *Socket) ioDone() {
	if 0 == atomic.AddInt32(&s.ioCount, -1) && s.testFlag(fdoclose) {
		if nil != s.b {
			s.b.Free()
		}
		if nil != s.inboundProcessor {
			s.inboundProcessor.OnSocketClose()
		}
		if nil != s.closeCallBack {
			s.closeCallBack(s, s.closeReason)
		}
	}
}

func (s *Socket) Close(reason error, delay time.Duration) {
	s.closeOnce.Do(func() {
		runtime.SetFinalizer(s, nil)

		s.muW.Lock()

		s.setFlag(fclosed)

		if !s.testFlag(fwclosed) && delay > 0 {
			if !s.sendLock {
				s.emitSendTask()
			}
			s.muW.Unlock()
			s.ShutdownRead()
			ticker := time.NewTicker(delay)
			go func() {
				select {
				case <-s.sendCloseChan:
				case <-ticker.C:
				}

				ticker.Stop()
				s.aioConn.Close(nil)
			}()
		} else {
			s.muW.Unlock()
			s.aioConn.Close(nil)
		}

		s.setFlag(fdoclose)
		s.closeReason = reason

		if 0 == atomic.LoadInt32(&s.ioCount) {
			if nil != s.b {
				s.b.Free()
			}
			if nil != s.inboundProcessor {
				s.inboundProcessor.OnSocketClose()
			}
			if nil != s.closeCallBack {
				s.closeCallBack(s, s.closeReason)
			}
		}
	})
}

func NewSocket(service *SocketService, conn net.Conn) *Socket {
	switch conn.(type) {
	case *net.TCPConn, *net.UnixConn:
		break
	default:
		return nil
	}

	c, err := service.createAIOConn(conn)
	if err != nil {
		return nil
	}

	s := &Socket{
		conn:          conn,
		sendCloseChan: make(chan struct{}),
		aioConn:       c,
		sendQueueSize: 256,
		sendQueue:     list.New(),
	}

	s.sendContext = ioContext{s: s, t: 's'}
	s.recvContext = ioContext{s: s, t: 'r'}

	runtime.SetFinalizer(s, func(s *Socket) {
		s.Close(errors.New("gc"), 0)
	})

	return s
}

func CreateSocket(conn net.Conn) *Socket {
	return NewSocket(aioService, conn)
}
