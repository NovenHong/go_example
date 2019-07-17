package main

import (
	"fmt"
	"time"
	"sync"
)

type Bucket struct {
	cap int
	token chan bool
	timer *time.Ticker
	mu sync.Mutex
}

func NewBucket(cap int,interval time.Duration) *Bucket {
	bucket := &Bucket{
		cap : cap,
		token : make(chan bool,cap),
		timer : time.NewTicker(interval),
	}
	go bucket.startTicker()
	return bucket
}

func (bucket *Bucket) startTicker() {
	for i := 0; i < bucket.cap; i++ {
		bucket.token <- true
	}
	for {
		select {
		case <-bucket.timer.C :
			for i := len(bucket.token); i < bucket.cap; i++ {
				bucket.Add()
			}
		}
	}
}

func (bucket *Bucket) Add() {
	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	if len(bucket.token) < bucket.cap {
		bucket.token <- true
	}
}

func (bucket *Bucket) Get() bool {
	select {
	case <-bucket.token :
		return true
	default :
		return false
	}
}


func main()  {
	
	bucket := NewBucket(5,time.Second)

	for i := 0; i < 100; i++ {
		time.Sleep(time.Millisecond * 100)
		go func(bucket *Bucket,index int){
			if bucket.Get() {
				fmt.Printf("#%d:get token success \n",index)
			}else{
				fmt.Printf("#%d:get token fail \n",index)
			}
		}(bucket,i)
	}

	for {

	}
}