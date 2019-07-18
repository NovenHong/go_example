package main

import (
	"fmt"
	"main/balance"
	"time"
)

func init() {
	
}

func main()  {

	var instances []*balance.Instance
	for i := 6; i <= 10; i++ {
		instance := balance.NewInstance(fmt.Sprintf("192.168.100.%d",i),8080)
		instances = append(instances,instance)
	}

	// instance,err := balance.DoBalance("random",instances)
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// fmt.Println(instance)


	for {
		instance,err := balance.DoBalance("roundrobin",instances)
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Println(instance)
		time.Sleep(time.Second*1)
	}
}