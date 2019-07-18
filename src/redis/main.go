package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"encoding/json"
	_ "time"
	"sync"
)

var rec redis.Conn
var err error
var wg sync.WaitGroup

func init() {
	rec,err = redis.Dial("tcp","127.0.0.1:6379")
	if err != nil {
		panic(err)
	}
	//defer rec.Close()

	res,err := rec.Do("PING")
	if res != `PONG`|| err != nil {
		panic("redis server exception")
	}

}

func main()  {

	wg.Add(1)
	go run()

	//time.Sleep(time.Second * 2)
	wg.Wait()

	is_exist,err := redis.Bool(rec.Do("EXISTS","mykey"))
	if !is_exist {
		_,err = rec.Do("SET","mykey","test","EX", "60")
		fmt.Println("create key mykey")
	}
	if err != nil {
		fmt.Println(err)
	}

	mykey,_ := redis.String(rec.Do("GET","mykey"))
	fmt.Println(mykey)

	var userinfo map[string]string
	// info := map[string]string {
	// 	"username" : "nana",
	// 	"address" : "western coast",
	// }
	userinfo = map[string]string {
		"username" : "nana",
		"address" : "western coast",
	}

	info_json,_ := json.Marshal(userinfo)
	fmt.Println(info_json)

	n,_ := rec.Do("LPUSH","userinfo",info_json)
	if n == int64(1) {
		fmt.Println("LPUSH success")
	}

	value,_ := redis.Bytes(rec.Do("LPOP","userinfo"))
	json.Unmarshal(value,&userinfo)

	fmt.Println(userinfo)

}

func run() {

	fmt.Println("run...")

	_,err = rec.Do("SET","mykey1","run test","EX", "60")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("create key mykey1")

	mykey,_ := redis.String(rec.Do("GET","mykey1"))
	fmt.Println(mykey)

	wg.Done()
}