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
	dist.InitZK(testService,stop,"")
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