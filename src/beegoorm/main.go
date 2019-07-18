package main

import (
	"fmt"
	"github.com/astaxie/beego/orm"
    _ "github.com/go-sql-driver/mysql" // import your used driver
)

type User struct {
	Id int
	Username string
	//Address *Address `orm:"rel(one)"`
}

type Address struct {
	Id int
	User_id int
	Addr string `orm:"column(address)"`
	//User *User `orm:"rel(fk)"`
}

var O orm.Ormer

func init() {
	// set default database
	orm.RegisterDataBase("default", "mysql", "root:@tcp(127.0.0.1:3306)/test?charset=utf8", 30)

	// register model
	orm.RegisterModel(new(User))
	orm.RegisterModel(new(Address))

    // create table
	//orm.RunSyncdb("default", false, true)
	
	O = orm.NewOrm()
}

func main()  {
	//fmt.Println("hello world")

	// user := User{Id:1}
	// err := O.Read(&user)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }

	// fmt.Println(user)

	// user := User{Username:"Swift taylor"}
	// id,err := O.Insert(&user)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }

	// address := Address{User_id:int(id),Addr:"New York"}
	// _,err = O.Insert(&address)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }

	// var user User
	// err := O.QueryTable("user").Filter("id",2).One(&user)
	// if err != nil {
	// 	fmt.Println(err)
	// }

	// var users []User
	// _,err := O.QueryTable("user").All(&users)
	// if err != nil {
	// 	fmt.Println(err)
	// }
	
	var maps []orm.Params
	sql := `
		select u.username,a.address from user u left join address a on u.id=a.user_id
	`
	O.Raw(sql).Values(&maps)
	fmt.Println(maps)
}