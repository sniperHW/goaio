/*
 * single wather,single complete routine
 */

package main

import (
	"fmt"
	"github.com/sniperHW/goaio"
	"net"
	"time"
)

var clientcount int32
var bytescount int32
var packetcount int32

func main() {

	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:8110")
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	go func() {
		for {
			conn, buff, bytestransfer, context, err := goaio.GetCompleteStatus()
			if nil != err {
				if err == goaio.ErrServiceClosed {
					return
				}
				fmt.Println(err)
				conn.Close()
			} else {
				if context.(rune) == 'r' {
					conn.Send(buff[:bytestransfer], 'w')
				} else {
					conn.Recv(buff[:cap(buff)], 'r')
				}
			}
		}
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}

		c, _ := goaio.Bind(conn)

		c.SetRecvTimeout(time.Second * 5)

		buff := make([]byte, 1024*4)

		c.Recv(buff, 'r')

	}

}
