package main

import (
	"fmt"
	"net"
	"bufio"
	"os"
	"strings"
)

var nickname string

func main()  {
	fmt.Println("hello world")

	conn,err := net.Dial("tcp","127.0.0.1:8081")
	if err != nil {
		fmt.Printf("connecting to server err:",err)
		return
	}
	defer conn.Close()

	go readConn(conn)

	fmt.Print("请输入昵称:")
	reader := bufio.NewReader(os.Stdin)
	line,_,_ := reader.ReadLine()
	nickname = strings.Title(string(line))
	fmt.Println("你的昵称是：",nickname)

	for {

		var msg string
		//fmt.Scanln(&msg)
		fmt.Print("请输入:")
		line,_,_ = reader.ReadLine()
		msg = string(line)

		fmt.Print("<" + nickname + ">" + "说:")
		fmt.Println(msg)

		data := []byte("<" + nickname + ">" + "说:" + msg)

		conn.Write(data)
	}
}

func readConn(conn net.Conn) {
	for {
		buff := make([]byte,256)
		total,err := conn.Read(buff)
		if err != nil {
			conn.Close()
			break
		}

		fmt.Println(string(buff[:total]))
	}
}