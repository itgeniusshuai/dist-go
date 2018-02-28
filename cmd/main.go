package main

import (
	"fmt"
	"../test"
	"../dist"
	"errors"
)

// 关闭信号
var Semaphore = make(chan int, 1)

func main() {
	zkList := []string{"192.168.201.219:2181","192.168.201.220:2181","192.168.201.218:2181"}
	dist.InitZK(testService,stop,zkList)
	fmt.Println("fsdfds")
	test.InitZK(testService)
	select {
	case <-Semaphore:
		fmt.Println("gogoogogo")
	}
}

func stop(){
	panic(errors.New("stop world"))
}

func testService(){
	fmt.Println("test")
}