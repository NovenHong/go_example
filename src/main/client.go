package main

import (
	"time"
	tp "github.com/henrylee2cn/teleport"
	"encoding/json"
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

	json_str,_ := json.Marshal(map[string]interface{}{
		"username" : "nana",
		"password" : 123456,
	})
	var result string
	stat = sess.Call("/user/register",
		json_str,
		&result,
	).Status()
	if !stat.OK() {
		tp.Fatalf("%v", stat)
	}
	tp.Printf("result: %v", result)

	tp.Printf("wait for 10s...")
	time.Sleep(time.Second * 10)

}