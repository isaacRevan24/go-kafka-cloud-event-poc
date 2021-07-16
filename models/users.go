package models

type User struct {
	Name     string `json:"name"`
	LastName string `json:"lastname"`
	Age      int    `json:"age"`
}
