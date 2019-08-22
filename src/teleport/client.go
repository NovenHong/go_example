package main

import (
	_ "time"
	tp "github.com/henrylee2cn/teleport"
	_ "encoding/json"
	"bufio"
	"fmt"
	"os"
)

type Push struct {
	tp.PushCtx
}

// Push handles '/push/status' message
func (p *Push) Status(arg *string) *tp.Status {
	tp.Printf("%s", *arg)
	return nil
}

func main()  {
	defer tp.SetLoggerLevel("ERROR")()

	cli := tp.NewPeer(tp.PeerConfig{
		//Network: "quic",
	})
	defer cli.Close()

	err := cli.SetTLSConfigFromFile("cert/cert.pem", "cert/key.pem", true)
	if err != nil {
		tp.Fatalf("%v", err)
	}
	//cli.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})

	cli.RoutePush(new(Push))

	sess, stat := cli.Dial(":9090")
	if !stat.OK() {
		tp.Fatalf("%v", stat)
	}

	//register api
	// json_str,_ := json.Marshal(map[string]interface{}{
	// 	"username" : "nana",
	// 	"password" : "123456",
	// })
	// var result string
	// stat = sess.Call("/user/register",
	// 	json_str,
	// 	&result,
	// ).Status()
	// if !stat.OK() {
	// 	tp.Fatalf("%v", stat)
	// }
	// tp.Printf("result: %v", result)

	//GetUsers api
	// var result string
	// stat = sess.Call("/user/getusers",
	// 	"",
	// 	&result,
	// ).Status()
	// if !stat.OK() {
	// 	tp.Fatalf("%v", stat)
	// }
	// tp.Printf("result: %v", result)

	//login api
	// json_str,_ := json.Marshal(map[string]interface{}{
	// 	"username" : "nana",
	// 	"password" : "123456",
	// })
	// var result []byte
	// stat = sess.Call("/user/login",
	// 	json_str,
	// 	&result,
	// ).Status()
	// if !stat.OK() {
	// 	tp.Fatalf("%v", stat)
	// }
	// tp.Printf("result: %s", result)

	// tp.Printf("wait for 10s...")
	// time.Sleep(time.Second * 10)

	reader := bufio.NewReader(os.Stdin)

	for {

		var msg string

		fmt.Print("请输入:")
		line,_,_ := reader.ReadLine()
		msg = string(line)

		if(msg == "exit"){
			// sess.AsyncCall(
			// 	"/user/close",
			// 	nil,
			// 	nil,
			// 	make(chan tp.CallCmd, 1),
			// )
			stat = sess.Call("/user/close", nil, nil).Status()
			if !stat.OK() {
				tp.Fatalf("%v", stat)
			}
		}else{
			fmt.Println("your message:%s",msg)
		}
	}
	

}