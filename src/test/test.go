package main

import (
	"fmt"
)

func main() {
	
	user1 := []string{"XTaIAWgGHZcDAN0oRDSisiLd","XTaIAWgGHZcDAN0oRDSisiLd","XT56XC/+udIDAJ/W5jtHfu0L","XTaIAWgGHZcDAN0oRDSisiLd","XTaIAWgGHZcDAN0oRDSisiLd"}
	user2 := []string{"XT56XC/+udIDAJ/W5jtHfu0L","XTaIAWgGHZcDAN0oRDSisiLd","XTaIAWgGHZcDAN0oRDSisiLd","XTaIAWgGHZcDAN0oRDSisiLd"}

	results := intersect(user1,user2)

	fmt.Println(results)
}


func intersect(slice1, slice2 []string) []string {
	m := make(map[string]int)
	nn := make([]string, 0)

	if len(slice1) <= len(slice2){
		for _, v := range slice1 {
			m[v]++
		}
	 
		for _, v := range slice2 {
			times, ok := m[v]
			if ok && times > 0 {
				nn = append(nn, v)
			}
		}
	}else {
		for _, v := range slice2 {
			m[v]++
		}

		fmt.Println(m)
	 
		for _, v := range slice1 {
			times, ok := m[v]
			if ok && times > 0 {
				nn = append(nn, v)
			}
		}
	}
	return nn
}