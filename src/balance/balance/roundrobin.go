package balance

import (
	"errors"
)

func init()  {
	RegisterBalance("roundrobin",&RoundRobinBalance{})
}

type RoundRobinBalance struct {
	curIndex int
}

func (p *RoundRobinBalance) DoBalance(instances [] *Instance,key ...string) (instance *Instance,err error) {
	if len(instances) == 0 {
		err = errors.New("no instance")
		return
	}

	length := len(instances)

	if p.curIndex >= length {
		p.curIndex = 0
	}
	instance = instances[p.curIndex]
	p.curIndex ++
	return
}