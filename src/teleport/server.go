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

var (
	SessionMap []tp.CtxSession
)

func (u *User) Login(arg *[]byte) ([]byte,*tp.Status) {

	var userinfo map[string]interface{}
	err := json.Unmarshal(*arg,&userinfo)
	if err != nil {
		return []byte(""),tp.NewStatus(1001, "json parse error", err.Error())
	}
	
	var result []byte

	if(userinfo["username"] == "nana" && userinfo["password"] == "123456"){
		sess := u.Session()
		SessionMap = append(SessionMap,sess)

		result,_ = json.Marshal(Success{Code:0,Message:"login success"})
		return result,nil
	}

	result,_ = json.Marshal(Error{Code:1001,Message:"username or password is not correct"})
	return result,nil
}

func (u *User) Register(arg *[]byte) (string,*tp.Status) {

	var userinfo map[string]interface{}
	err := json.Unmarshal(*arg,&userinfo)
	if err != nil {
		return "",tp.NewStatus(1001, "json parse error", err.Error())
	}

	userinfo["id"] = 0
	userinfo["status"] = 1
	userinfo["createTime"] = time.Now().Unix()

	user := model.NewUserModel(userinfo)
	id,err := user.Create()
	if err != nil {
		return "",tp.NewStatus(1001, "mysql error", err.Error())
	}

	var result []byte

	if(id > 0){
		result,_ = json.Marshal(Success{Code:0,Message:fmt.Sprintf("user id = %d",id)})
	}else{
		result,_ = json.Marshal(Error{Code:1001,Message:"register user fail"})
	}
	return string(result),nil
}

func (u *User) Getusers(arg *[]byte) (string,*tp.Status) {

	user := model.NewUserModel(make(map[string]interface{}))

	users,err := user.Select()
	if err != nil {
		return "",tp.NewStatus(1001, "mysql error", err.Error())
	}

	var result []byte
	result,_ = json.Marshal(Success{Code:0,Message:"",Data:users})

	return string(result),nil
}

func (u *User) Close(arg *[]byte) (interface{},*tp.Status) {

	fmt.Println(<-u.Session().CloseNotify())
	
	return nil, nil
}

func main() {
	defer tp.FlushLogger()

	go tp.GraceSignal()

	srv := tp.NewPeer(tp.PeerConfig{
		CountTime:   true,
		ListenPort:  9090,
		PrintDetail: false,
		//DefaultSessionAge: time.Second * 60,
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