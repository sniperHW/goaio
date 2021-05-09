package goaio

import (
	"container/list"
	"reflect"
	"sync"
	"unsafe"
)

const hashMask int = 8
const hashSize int = 1 << hashMask

/*type fd2Conn []sync.Map

func (self *fd2Conn) add(conn *AIOConn) {
	(*self)[conn.fd>>hashMask].Store(conn.fd, reflect.ValueOf(conn).Pointer())
}

func (self *fd2Conn) get(fd int) (*AIOConn, bool) {
	v, ok := (*self)[fd>>hashMask].Load(fd)
	if ok {
		return (*AIOConn)(unsafe.Pointer(v.(uintptr))), true
	} else {
		return nil, false
	}
}

func (self *fd2Conn) remove(conn *AIOConn) {
	(*self)[conn.fd>>hashMask].Delete(conn.fd)
}*/

type mapConn struct {
	mu    sync.RWMutex
	conns map[int]uintptr
}

type fd2Conn struct {
	mapConns [hashMask]*mapConn
}

func newFd2Conn() fd2Conn {
	r := fd2Conn{}
	for k, _ := range r.mapConns {
		r.mapConns[k] = &mapConn{
			conns: map[int]uintptr{},
		}
	}
	return r
}

func (self *fd2Conn) add(conn *AIOConn) {
	m := self.mapConns[conn.fd>>hashMask]
	m.mu.Lock()
	m.conns[conn.fd] = reflect.ValueOf(conn).Pointer()
	m.mu.Unlock()
}

func (self *fd2Conn) get(fd int) (*AIOConn, bool) {
	m := self.mapConns[fd>>hashMask]
	m.mu.RLock()
	v, ok := m.conns[fd]
	m.mu.RUnlock()
	if ok {
		return (*AIOConn)(unsafe.Pointer(v)), true
	} else {
		return nil, false
	}
}

func (self *fd2Conn) remove(conn *AIOConn) {
	m := self.mapConns[conn.fd>>hashMask]
	m.mu.Lock()
	delete(m.conns, conn.fd)
	m.mu.Unlock()
}

type pendingWatch struct {
	conn *AIOConn
	resp chan bool
}

type poller_base struct {
	fd        int
	fd2Conn   fd2Conn
	ver       int64
	die       chan struct{}
	muPending sync.Mutex
	pending   *list.List
}

type pollerI interface {
	close()
	trigger() error
	watch(*AIOConn) <-chan bool
	unwatch(*AIOConn) bool
	wait(*int32)
}
