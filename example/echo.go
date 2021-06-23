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
			res, ok := goaio.GetCompleteStatus()
			if !ok {
				return
			} else if nil != res.Err {
				fmt.Println("go error", res.Err)
				res.Conn.Close(res.Err)
			} else if res.Context.(rune) == 'r' {
				fmt.Println("on recv")
				res.Conn.Send('w', res.Buff[:res.Bytestransfer], -1)
			} else {
				fmt.Println("on send")
				res.Conn.Recv('r', res.Buff[:cap(res.Buff)], time.Second*5)
			}
		}
	}()

	fmt.Println("server start at localhost:8110")

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println("new client")

		c, _ := goaio.CreateAIOConn(conn, goaio.AIOConnOption{})

		c.Recv1('r', make([]byte, 1024*4), time.Second*5)

	}

}
