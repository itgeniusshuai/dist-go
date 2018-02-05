package main

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"sort"
	"time"
	"sync"
)

// 注册中心列表
var zkList = []string{"192.168.201.219:2181", "192.168.201.220:2181", "192.168.201.218:2181"}

// 同步服务持久化节点
const parentPath = "/async_app"

// 临时节点名称
const tmpPath = parentPath + "/app"

// 是否是主服务器
var isMaster bool

// 是否已启动
var isRunning bool = false

// 活着的节点
var activeList []string

// 当前节点
var currentNode string

// 主节点名称
var masterNode string

// 与zk的连接
var conn *zk.Conn

// 关闭信号
var Semaphore = make(chan int,1)

// 运行锁，防止程序运行两遍
var lock sync.Mutex

func main() {
	testGetMinActiveNode()
}

func initZK() {
	//  注册zookeeper
	conn, _, err := zk.Connect(zkList, 15*time.Second)
	if err != nil {
		fmt.Println("zk connect error")
		return
	}
	isExist, _,e, err := conn.ExistsW(parentPath)
	if err != nil {
		fmt.Println("zk parent path query error [%s]", err.Error())
		return
	}
	if !isExist {
		// 创建持久化节点
		_, err := conn.Create(parentPath, []byte("async"), 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			println("zk crate parent [%s] path error [%s]", parentPath, err.Error())
			return
		}
	}
	// 注册监听
	go watchNodeEvent(e)
	// 创建临时有序节点
	currentNode, err = conn.Create(tmpPath, []byte("client"), 3, zk.WorldACL(zk.PermAll))

	// 获取所有列表
	flushActiveList()

	// 选主并运行主节点
	electAndRun()

	select {
	case <-Semaphore:
		fmt.Println("close process")
	}

}

func watchNodeEvent(e <-chan zk.Event){
	event:=<-e
	fmt.Println("path:", event.Path)
	fmt.Println("type:", event.Type.String())
	fmt.Println("state:", event.State.String())
	// 刷新可用节点列表
	flushActiveList()
	// 选主并运行主节点
	electAndRun()
	// 重新注册监听
	_, _,e, err := conn.ExistsW(parentPath)
	if err != nil{
		println("zk parent path query error [%s] when rereigister", err.Error())
	}
	go watchNodeEvent(e)
}

func flushActiveList(){
	list, _, err := conn.Children(parentPath)
	if err != nil {
		println("zk create tmp node error %s", err.Error())
		return
	}
	activeList = list
}

func electAndRun(){
	// 获取最小节点
	masterNode = getMinActiveNode(activeList)
	// 是否是主
	isMaster = masterNode == currentNode
	// 如果是主且当前不是运行状态，启动
	lock.Lock()
	defer lock.Unlock()
	if isMaster && !isRunning{
		go doService()
		isRunning = true
	}
}

func doService(){
	fmt.Println(currentNode + " are running")
}

func getMinActiveNode(activeNodes []string) string {
	sort.Strings(activeNodes)
	return activeNodes[0]
}

func testGetMinActiveNode() {
	var activeNodes = []string{"app00002", "app00001"}
	fmt.Println(getMinActiveNode(activeNodes))
}
