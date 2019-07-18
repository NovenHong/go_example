package balance

import (
	"errors"
	"math/rand"
	_ "fmt"
	"time"
)

type RandomBalance struct {

}

func init() {
	RegisterBalance("random",&RandomBalance{})
	rand.Seed(time.Now().UnixNano())
}

func (p *RandomBalance) DoBalance(instances []*Instance,key ...string) (instance *Instance,err error) {
	if len(instances) == 0 {
		err = errors.New("no instance")
		return
	}

	length := len(instances)

	index := rand.Intn(length)

	//fmt.Println(index)

	instance = instances[index]

	return
}