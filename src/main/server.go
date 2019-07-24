package main

import (
	"fmt"
	"time"
	"encoding/json"
	tp "github.com/henrylee2cn/teleport"
	"main/model"
)

type Success struct {
	Code int `json:"code"`
	Message string `json:"message"`
	Data interface{} `json:"data"`
}

type Error struct {
	Code int `json:"code"`
	Message string `json:"message"`
}

type User struct {
	tp.CallCtx
}

func (u *User) Login(arg *[]byte) (string,*tp.Status) {

	var userinfo map[string]interface{}
	err := json.Unmarshal(*arg,&userinfo)
	if err != nil {
		return "",tp.NewStatus(1001, "json parse error", err.Error())
	}
	fmt.Println(userinfo)
	
	var result []byte

	if(userinfo["username"] == "nana" && (userinfo["password"]).(float64) == 123456){
		result,_ = json.Marshal(Success{Code:0,Message:"login ok"})
		return string(result),nil
	}

	result,_ = json.Marshal(Error{Code:1001,Message:"username or password is not correct"})
	return string(result),nil
}

func (u *User) Register(arg *[]byte) (string,*tp.Status) {
	user := model.NewUserModel(map[string]interface{}{
		"username" : "nana",
		"password" : "123456",
		"status" : 1,
		"createTime" : time.Now().Unix(),
	})
	id,err := user.Create()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("mysql id:%d",id)
	return "",nil
}

func main() {
	defer tp.FlushLogger()

	go tp.GraceSignal()

	srv := tp.NewPeer(tp.PeerConfig{
		//Network:     "quic",
		CountTime:   true,
		ListenPort:  9090,
		PrintDetail: true,
	})

	err := srv.SetTLSConfigFromFile("cert/cert.pem", "cert/key.pem")
	if err != nil {
		tp.Fatalf("%v", err)
	}

	srv.RouteCall(new(User))

	go func() {
		for {
			time.Sleep(time.Second * 5)
			var session_id string
			srv.RangeSession(func(sess tp.Session) bool {
				session_id = sess.ID()
				// sess.Push(
				// 	"/push/status",
				// 	fmt.Sprintf("this is a broadcast, server time: %v", time.Now()),
				// )
				return true
			})

			//sess,ok := srv.GetSession(session_id)
			//fmt.Println(sess,ok)
		}
	}()

	srv.ListenAndServe()
}