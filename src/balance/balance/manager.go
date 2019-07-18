package balance

import (
	"fmt"
)

type BalanceMgr struct {
	allBalance map[string]Balance
}

var mgr = BalanceMgr{
	allBalance : make(map[string]Balance),
}

func (p *BalanceMgr) registerBalance(name string,balance Balance) {
	p.allBalance[name] = balance
}

func RegisterBalance(name string,balance Balance) {
	mgr.registerBalance(name,balance)
}

func DoBalance(name string,instances [] *Instance) (instance *Instance,err error) {
	balance := mgr.allBalance[name]

	instance,err = balance.DoBalance(instances)
	if err != nil {
		err = fmt.Errorf("%s error:%s \n",name,err)
		return
	}
	return
}