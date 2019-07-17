package main

import (
	"fmt"
	"net"
)

var ConnMap map[string]net.Conn

func main()  {
	
	server,err := net.Listen("tcp",":8081")
	if err != nil {
		panic(err)
	}

	fmt.Printf("server listen on %s \n",server.Addr().String())

	ConnMap = make(map[string]net.Conn)

	for {
		conn,_ := server.Accept()
		defer conn.Close()

		ConnMap[conn.RemoteAddr().String()] = conn
		fmt.Printf("连接客户端信息:%s \n",conn.RemoteAddr().String())

		go handleConn(conn)
	}

}

func handleConn(conn net.Conn) {
	for {
		buf := make([]byte,256)
		total,err := conn.Read(buf)
		if total == 0 || err != nil {
			conn.Close()
			break
		}

		fmt.Println(string(buf[:total]))

		for _,c := range ConnMap {
			if c.RemoteAddr().String() == conn.RemoteAddr().String() {
				continue
			}
			c.Write(buf[:total])
		}
	}
}