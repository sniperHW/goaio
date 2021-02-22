package main

import (
	"fmt"
	"github.com/sniperHW/goaio/example/wrap"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/golog"
	"github.com/sniperHW/kendynet/timer"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"
)

type ByteMessage struct {
	bytes []byte
}

func (this *ByteMessage) Bytes() []byte {
	return this.bytes
}

type EnCoder struct {
}

func (this EnCoder) EnCode(o interface{}) (wrap.Message, error) {
	return &ByteMessage{bytes: o.([]byte)}, nil
}

var socketService *wrap.SocketSerice

func server(service string) {

	clientcount := int32(0)
	bytescount := int32(0)
	packetcount := int32(0)

	timer.Repeat(time.Second, func(_ *timer.Timer, ctx interface{}) {
		tmp1 := atomic.LoadInt32(&bytescount)
		tmp2 := atomic.LoadInt32(&packetcount)
		atomic.StoreInt32(&bytescount, 0)
		atomic.StoreInt32(&packetcount, 0)
		fmt.Printf("clientcount:%d,transrfer:%d KB/s,packetcount:%d\n", atomic.LoadInt32(&clientcount), tmp1/1024, tmp2)
	}, nil)

	tcpAddr, err := net.ResolveTCPAddr("tcp", service)
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			atomic.AddInt32(&clientcount, 1)
			s := wrap.NewSocket(socketService, conn)
			s.SetRecvTimeout(time.Second * 5)
			s.SetCloseCallBack(func(_ *wrap.Socket, reason error) {
				atomic.AddInt32(&clientcount, -1)
				fmt.Println("client close:", reason, atomic.LoadInt32(&clientcount))
			})
			s.SetEncoder(EnCoder{})
			s.BeginRecv(func(s *wrap.Socket, data interface{}) {
				atomic.AddInt32(&bytescount, int32(len(data.([]byte))))
				atomic.AddInt32(&packetcount, int32(1))
				//fmt.Println(string(data.([]byte)), len(data.([]byte)))
				s.Send(data)
			})
		}
	}()
}

func client(service string, count int) {

	for i := 0; i < count; i++ {
		conn, err := net.Dial("tcp", service)
		if err != nil {
			panic(err)
		}
		s := wrap.NewSocket(socketService, conn)
		s.SetEncoder(EnCoder{})
		s.BeginRecv(func(s *wrap.Socket, data interface{}) {
			s.Send(data)
		})

		//send the first messge
		msg := []byte("hello")
		s.Send(msg)
		s.Send(msg)
		s.Send(msg)

	}
}

func main() {

	go func() {
		http.ListenAndServe("0.0.0.0:6060", nil)
	}()

	outLogger := golog.NewOutputLogger("log", "kendynet", 1024*1024*1000)
	kendynet.InitLogger(golog.New("rpc", outLogger))

	_ = runtime.NumCPU() * 2

	socketService = wrap.NewSocketSerice(nil)

	if len(os.Args) < 3 {
		fmt.Printf("usage ./pingpong [server|client|both] ip:port clientcount\n")
		return
	}

	mode := os.Args[1]

	if !(mode == "server" || mode == "client" || mode == "both") {
		fmt.Printf("usage ./pingpong [server|client|both] ip:port clientcount\n")
		return
	}

	service := os.Args[2]

	sigStop := make(chan bool)

	if mode == "server" || mode == "both" {
		go server(service)
	}

	if mode == "client" || mode == "both" {
		if len(os.Args) < 4 {
			fmt.Printf("usage ./pingpong [server|client|both] ip:port clientcount\n")
			return
		}
		connectioncount, err := strconv.Atoi(os.Args[3])
		if err != nil {
			fmt.Printf(err.Error())
			return
		}
		//让服务器先运行
		time.Sleep(10000000)
		go client(service, connectioncount)

	}

	_, _ = <-sigStop

	return

}
