package model

import (
	_ "fmt"
)

type User struct {
	Id int
	Username string
	Password string
	Status int
	CreateTime int64
}

func NewUserModel(user map[string]interface{}) (*User) {
	if(len(user) == 0){
		return &User{}
	}
	return &User {
		Id : user["id"].(int),
		Username : user["username"].(string),
		Password : user["password"].(string),
		Status : user["status"].(int),
		CreateTime : user["createTime"].(int64),
	}
}

func (user *User) Create() (id int64,err error) {
	stmt, err := db.Prepare("insert user set username=?,password=?,status=?,create_time=?")
	res, err := stmt.Exec(user.Username,user.Password,user.Status,user.CreateTime)
	id, err = res.LastInsertId()
	return
}

func (user *User) Select() (users []*User,err error) {
	rows, err := db.Query("SELECT id,username,status,create_time FROM user")
	
	for rows.Next() {

		err = rows.Scan(&user.Id,&user.Username,&user.Status,&user.CreateTime)

		users = append(users,user)
	}

	return
}