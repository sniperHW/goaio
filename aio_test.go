//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out
//ulimit -n 1048576
//go test -v -run=^$ -bench BenchmarkEcho128KParallel -count 100
package goaio

import (
	"bytes"
	"crypto/rand"
	//"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

type TestBufferPool struct {
	pool chan []byte
}

func NewBufferPool(bufsize int) *TestBufferPool {
	size := 1
	p := &TestBufferPool{
		pool: make(chan []byte, size),
	}
	for i := 0; i < size; i++ {
		p.pool <- make([]byte, bufsize)
	}
	return p
}

func (p *TestBufferPool) Acquire() []byte {
	return <-p.pool
}

func (p *TestBufferPool) Release(buff []byte) {
	p.pool <- buff[:cap(buff)]
}

func init() {

	go http.ListenAndServe(":6060", nil)
}

const (
	bufSize = 65536
)

var resultCount int32

func echoServer(t testing.TB, bufsize int) (net.Listener, chan struct{}) {

	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	w := NewAIOService(1)

	die := make(chan struct{})

	var clientCount int32

	go func() {
		for {
			conn, buff, bytestransfer, context, err := w.GetCompleteStatus()
			if nil != err {
				if err == ErrServiceClosed {
					break
				} else {
					//fmt.Println("close conn", conn.fd, err)
					conn.Close(err)
					atomic.AddInt32(&clientCount, -1)
				}
			} else {
				if context.(rune) == 'r' {
					//fmt.Println("recv", bytestransfer)
					conn.Send(buff[:bytestransfer], 'w')
				} else {
					conn.Recv(buff[:cap(buff)], 'r')
				}
			}

			if 0 == atomic.LoadInt32(&clientCount) {
				break
			}
		}
		//fmt.Println("server break")
		w.Close()
		close(die)
	}()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			c, err := w.Bind(conn, AIOConnOption{})
			if err != nil {
				panic(err)
				w.Close()
				return
			}

			atomic.AddInt32(&clientCount, 1)

			//c.SetRecvTimeout(time.Second)

			buff := make([]byte, bufsize)
			if err := c.Recv(buff, 'r'); nil != err {
				fmt.Println("first recv", err, "fd", c.fd)
				panic("panic")
			}
		}
	}()
	return ln, die
}

func TestDefault(t *testing.T) {
	ln, serverDie := echoServer(t, 4096)

	defer func() {
		ln.Close()
		<-serverDie
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	c, err := Bind(conn, AIOConnOption{})

	if nil != err {
		t.Fatal(err)
	}

	wx := make([]byte, 4096)

	c.Send(wx, 'w')

	for {
		conn, _, _, context, err := GetCompleteStatus()
		if nil == err {
			if context.(rune) == 'w' {
				conn.Close(ErrActiveClose)
				break
			}
		}
	}
}

func TestSendMutilBuff(t *testing.T) {
	ln, serverDie := echoServer(t, 4096)

	defer func() {
		ln.Close()
		<-serverDie
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	c, err := Bind(conn, AIOConnOption{})

	if nil != err {
		t.Fatal(err)
	}

	wx := make([]byte, 1024)

	c.Send(wx, 'w')
	c.Send(wx, 'w')
	c.Send(wx, 'w')
	c.Send(wx, 'w')
	c.Send(wx, 'w')

	rx := make([]byte, 4096)
	c.Recv(rx, 'r')

	cc := 0

	for {
		conn, _, _, context, err := GetCompleteStatus()
		if nil == err {
			if context.(rune) == 'w' {
				cc++
				if cc == 5 {
					conn.Close(ErrActiveClose)
					break
				}
			} else {
				c.Recv(rx, 'r')
			}
		}
	}
}

func TestGC(t *testing.T) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	conn.(*net.TCPConn).SetWriteBuffer(4096)

	w := NewAIOService(1)

	defer w.Close()

	_, err = w.Bind(conn, AIOConnOption{})

	if nil != err {
		t.Fatal(err)
	}

	runtime.GC()

	ln.Close()
}

func TestClose(t *testing.T) {
	ln, serverDie := echoServer(t, 4096)

	defer func() {
		ln.Close()
		<-serverDie
	}()

	w := NewAIOService(1)

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	c, err := w.Bind(conn, AIOConnOption{})

	if nil != err {
		t.Fatal(err)
	}

	_, err = w.Bind(conn, AIOConnOption{})

	assert.Equal(t, ErrWatchFailed, err)

	w.Close()

	_, err = w.Bind(conn, AIOConnOption{})

	assert.Equal(t, ErrServiceClosed, err)

	c.Close(ErrActiveClose)
}

func TestShareBuffer(t *testing.T) {
	ln, serverDie := echoServer(t, 4096)

	defer func() {
		ln.Close()
		<-serverDie
	}()

	w := NewAIOService(1)

	defer w.Close()

	buffpool := NewBufferPool(4096)

	for i := 0; i < 10; i++ {

		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}

		c, err := w.Bind(conn, AIOConnOption{ShareBuff: buffpool})

		if nil != err {
			t.Fatal(err)
		}

		wx := make([]byte, 4096)

		c.Send(wx, 'w')

		defer c.Close(ErrActiveClose)
	}

	count := 0

	for {
		conn, buff, _, context, err := w.GetCompleteStatus()
		if nil == err {
			if context.(rune) == 'w' {
				//使用ShareBuff,不需要提供buff
				conn.Recv(nil, 'r')
			} else {
				//使用关闭归还buffpool供其它连接使用
				buffpool.Release(buff)
				count++
				if count == 10 {
					break
				}
			}
		}
	}
}

func TestRecvTimeout1(t *testing.T) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	clients := []net.Conn{}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			clients = append(clients, conn)
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	w := NewAIOService(1)

	c, err := w.Bind(conn, AIOConnOption{})

	if nil != err {
		t.Fatal(err)
	}

	rx := make([]byte, 4096)

	c.Recv(rx, 'r')

	c.SetRecvTimeout(time.Second)

	rx = make([]byte, 4096)

	c.Recv(rx, 'r')

	count := 0

	for {
		conn, _, _, _, err := w.GetCompleteStatus()
		if nil != err {
			fmt.Println(err)
			if err != ErrRecvTimeout {
				panic("err type mismatch")
			}
			count++
			if count == 2 {
				conn.Close(ErrActiveClose)
				break
			}
		}
	}

	ln.Close()
	w.Close()
	for _, v := range clients {
		v.Close()
	}
}

func TestRecvTimeout2(t *testing.T) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	clients := []net.Conn{}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			clients = append(clients, conn)
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	w := NewAIOService(1)

	c, err := w.Bind(conn, AIOConnOption{})

	if nil != err {
		t.Fatal(err)
	}

	rx := make([]byte, 4096)

	c.Recv(rx, 'r')

	c.SetRecvTimeout(time.Second)

	rx = make([]byte, 4096)

	c.Recv(rx, 'r')

	c.SetRecvTimeout(0)

	die := make(chan struct{})

	go func() {
		for {
			_, _, _, _, err := w.GetCompleteStatus()
			if nil != err {
				if err == ErrServiceClosed {
					break
				} else if err == ErrRecvTimeout {
					panic("err type mismatch")
				}
			}
		}
		close(die)
	}()

	newTimer(time.Second*2, func(*Timer) {
		w.Close()
	})

	<-die

	ln.Close()
	for _, v := range clients {
		v.Close()
	}

}

func TestRecvTimeout3(t *testing.T) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	clients := []net.Conn{}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			clients = append(clients, conn)
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	w := NewAIOService(1)

	c, err := w.Bind(conn, AIOConnOption{UserData: 1})

	if nil != err {
		t.Fatal(err)
	}

	assert.Equal(t, 1, c.GetUserData().(int))

	c.SetRecvTimeout(time.Second)

	rx := make([]byte, 4096)

	c.Recv(rx, 'r')

	die := make(chan struct{})

	go func() {
		for {
			_, _, _, _, err := w.GetCompleteStatus()
			if nil != err {
				if err == ErrServiceClosed {
					break
				} else if err == ErrRecvTimeout {
					c.Close(nil)
				}
			}
		}
		close(die)
	}()

	newTimer(time.Second*2, func(*Timer) {
		assert.Equal(t, c.Recv(rx, 'r'), ErrConnClosed)
		assert.Equal(t, c.Send(rx, 'r'), ErrConnClosed)
		w.Close()
	})

	<-die

	ln.Close()
	for _, v := range clients {
		v.Close()
	}

}

func TestSendTimeout1(t *testing.T) {

	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	clients := []net.Conn{}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.(*net.TCPConn).SetReadBuffer(4096)
			clients = append(clients, conn)
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	conn.(*net.TCPConn).SetWriteBuffer(4096)

	w := NewAIOService(1)

	c, err := w.Bind(conn, AIOConnOption{})

	if nil != err {
		t.Fatal(err)
	}

	wx := make([]byte, 4096)

	c.Send(wx, 'w')

	c.SetSendTimeout(time.Second)

	c.Send(wx, 'w')

	for {
		conn, _, _, _, err := w.GetCompleteStatus()
		if nil != err {
			if err != ErrSendTimeout {
				panic("err type mismatch")
			}
			conn.Close(err)
			break
		} else {
			c.Send(wx, 'w')
		}
	}

	ln.Close()
	w.Close()
	for _, v := range clients {
		v.Close()
	}
}

func TestSendTimeout2(t *testing.T) {

	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	clients := []net.Conn{}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.(*net.TCPConn).SetReadBuffer(4096)
			clients = append(clients, conn)
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	conn.(*net.TCPConn).SetWriteBuffer(4096)

	w := NewAIOService(0)

	c, err := w.Bind(conn, AIOConnOption{})

	if nil != err {
		t.Fatal(err)
	}

	wx := make([]byte, 4096)

	c.SetSendTimeout(time.Second)

	c.Send(wx, 'w')

	c.SetSendTimeout(0)

	die := make(chan struct{})

	go func() {
		for {
			conn, _, _, _, err := w.GetCompleteStatus()
			if nil != err {
				if err == ErrServiceClosed {
					break
				} else if err == ErrRecvTimeout {
					panic("err type mismatch")
				}
			} else {
				conn.Send(wx, 'w')
			}
		}
		close(die)
	}()

	newTimer(time.Second*2, func(*Timer) {
		w.Close()
	})

	<-die

	ln.Close()

	for _, v := range clients {
		v.Close()
	}

	assert.Equal(t, ErrServiceClosed, c.Send(wx, nil))
	assert.Equal(t, ErrServiceClosed, c.Recv(wx, nil))

}

func TestEchoTiny(t *testing.T) {
	ln, serverDie := echoServer(t, 4096)

	defer func() {
		ln.Close()
		<-serverDie
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	tx := []byte("hello world")
	rx := make([]byte, len(tx))

	_, err = conn.Write(tx)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("tx:", string(tx))
	_, err = conn.Read(rx)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("rx:", string(tx))
	conn.Close()
}

func TestEchoHuge(t *testing.T) {
	ln, serverDie := echoServer(t, 4096)

	defer func() {
		ln.Close()
		<-serverDie
	}()

	tx := make([]byte, 100*1024*1024)
	n, err := io.ReadFull(rand.Reader, tx)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		n, err := conn.Write(tx)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("ping size", n)
	}()

	rx := make([]byte, len(tx))
	n, err = io.ReadFull(conn, rx)
	if err != nil {
		t.Fatal(err, n)
	}
	t.Log("pong size:", n)

	if bytes.Compare(tx, rx) != 0 {
		t.Fatal("incorrect receiving")
	}
	conn.Close()
}

func Test100(t *testing.T) {
	testParallel(t, 10, 1024)
}

/*func Test1k(t *testing.T) {
	testParallel(t, 1024, 1024)
}
func Test2k(t *testing.T) {
	testParallel(t, 2048, 1024)
}*/

/*func Test4k(t *testing.T) {
	testParallel(t, 4096, 1024, false)
}*/

/*
func Test8k(t *testing.T) {
	testParallel(t, 8192, 1024)
}

func Test10k(t *testing.T) {
	testParallel(t, 10240, 1024)
}

func Test12k(t *testing.T) {
	testParallel(t, 12288, 1024)
}*/

/*func Test1kTiny(t *testing.T) {
	testParallel(t, 1024, 16)
}

func Test2kTiny(t *testing.T) {
	testParallel(t, 2048, 16)
}*/

/*func Test4kTiny(t *testing.T) {
	testParallel(t, 4096, 16, false)
}*/

func testParallel(t *testing.T, par int, msgsize int) {
	t.Log("testing concurrent:", par, "connections")
	ln, serverDie := echoServer(t, msgsize)
	defer func() {
		t.Log("wait server finish")
		ln.Close()
		<-serverDie
	}()

	w := NewAIOService(1)

	die := make(chan struct{})

	ok := make(chan struct{})

	go func() {

		conns := []*AIOConn{}

		for i := 0; i < par; i++ {
			data := make([]byte, msgsize)
			conn, err := net.Dial("tcp", ln.Addr().String())

			if err != nil {
				log.Fatal(err)
			}

			c, err := w.Bind(conn, AIOConnOption{})
			if err != nil {
				t.Fatal(err)
			}

			conns = append(conns, c)

			// send
			err = c.Send(data, 'w')
			if err != nil {
				panic(err)
				log.Fatal(err)
			}
		}
		<-die

		for _, v := range conns {
			v.Close(ErrActiveClose)
		}

		close(ok)

	}()

	nbytes := 0
	ntotal := msgsize * par

	go func() {

		for {
			conn, buff, bytestransfer, context, err := w.GetCompleteStatus()
			if nil != err {
				if err == ErrServiceClosed {
					break
				} else {
					conn.Close(err)
				}
			} else {
				if context.(rune) == 'r' {
					nbytes += bytestransfer
					if nbytes >= ntotal {
						t.Log("completed:", nbytes)
						close(die)
						break
					}
				} else {
					conn.Recv(buff[:cap(buff)], 'r')
				}
			}
		}
	}()

	<-ok
}

func BenchmarkEcho128B(b *testing.B) {
	benchmarkEcho(b, 128, 1)
}

func BenchmarkEcho1K(b *testing.B) {
	benchmarkEcho(b, 1024, 1)
}

func BenchmarkEcho4K(b *testing.B) {
	benchmarkEcho(b, 4096, 1)
}

func BenchmarkEcho64K(b *testing.B) {
	benchmarkEcho(b, 65536, 1)
}

func BenchmarkEcho128K(b *testing.B) {
	benchmarkEcho(b, 128*1024, 1)
}

/*
func BenchmarkEcho128BParallel(b *testing.B) {
	benchmarkEcho(b, 128, 128)
}

func BenchmarkEcho1KParallel(b *testing.B) {
	benchmarkEcho(b, 1024, 128)
}

func BenchmarkEcho4KParallel(b *testing.B) {
	benchmarkEcho(b, 4096, 128)
}

func BenchmarkEcho64KParallel(b *testing.B) {
	benchmarkEcho(b, 65536, 128)
}

*/
func BenchmarkEcho128KParallel(b *testing.B) {
	benchmarkEcho(b, 128*1024, 32)
}

func benchmarkEcho(b *testing.B, bufsize int, numconn int) {

	b.Log("benchmark echo with message size:", bufsize, "with", numconn, "parallel connections, for", b.N, "times")

	ln, serverDie := echoServer(b, bufsize)
	defer func() {
		ln.Close()
		<-serverDie
	}()

	w := NewAIOService(1)

	defer w.Close()

	addr, _ := net.ResolveTCPAddr("tcp", ln.Addr().String())
	for i := 0; i < numconn; i++ {
		rx := make([]byte, bufsize)
		tx := make([]byte, bufsize)
		conn, err := net.DialTCP("tcp", nil, addr)
		if err != nil {
			b.Fatal("dial:", err)
			return
		}

		c, err := w.Bind(conn, AIOConnOption{})
		if err != nil {
			b.Fatal(err)
		}

		c.Send(tx, 'w')
		c.Recv(rx, 'r')
		defer c.Close(ErrActiveClose)
	}

	b.ReportAllocs()
	b.SetBytes(int64(bufsize * numconn))
	b.ResetTimer()

	count := 0
	target := bufsize * b.N * numconn
	sendsize := 0

	for {
		conn, buff, bytestransfer, context, err := w.GetCompleteStatus()
		if nil != err {
			if err == ErrServiceClosed {
				break
			} else {
				conn.Close(err)
			}
		} else {
			if context.(rune) == 'r' {
				count += bytestransfer
				if count >= target {
					break
				}
				conn.Send(buff[:bytestransfer], 'w')
			} else {
				sendsize += bytestransfer
				conn.Recv(buff[:cap(buff)], 'r')
			}
		}
	}
}
