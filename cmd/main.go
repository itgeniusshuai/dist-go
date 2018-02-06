package main

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"sort"
	"sync"
	"time"
	"../test"
)

// 注册中心列表
var config Config

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
var Semaphore = make(chan int, 1)

// 运行锁，防止程序运行两遍
var lock sync.Mutex

// 业务调研方法
var sf func()

// 配置文件路径
var configPath string = "etc/config.yml"

func main() {
	initZK(testService,"")
	fmt.Println("fsdfds")
	test.InitZK(testService)
	select {
	case <-Semaphore:
		fmt.Println("close process")
	}
}
func testService(){
	fmt.Println("test")
}
type Config struct {
	ZkList []string `yaml:"zkList"`
}

func initConfig() {
	configByte, err := ioutil.ReadFile(configPath)
	if err != nil {
		fmt.Println(err)
	}
	config = Config{}
	yaml.Unmarshal(configByte, &config)
	fmt.Println("zk list ", config.ZkList)
}

func initZK(f func(), configFilePath string) {
	if configFilePath != ""{
		configPath = configFilePath
	}
	//初始zk配置
	initConfig()
	// 初始化业务方法
	sf = f

	// 与zookeeper回调
	zkCallBack := zk.WithEventCallback(watchZK)
	//  注册zookeeper
	c, _, err := zk.Connect(config.ZkList, 15*time.Second,zkCallBack)
	conn = c
	if err != nil {
		fmt.Println("zk connect error")
		return
	}
	isExist, _, e, err := conn.ExistsW(parentPath)
	if err != nil {
		PrintStr("zk parent path query error [%s]", err.Error())
		return
	}
	if !isExist {
		// 创建持久化节点
		_, err := conn.Create(parentPath, []byte("async"), 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			PrintStr("zk crate parent [%s] path error [%s]", parentPath, err.Error())
			return
		}
	}
	// 注册监听
	_, _, e, err = conn.ChildrenW(parentPath)
	go watchNodeEvent(e)
	// 创建临时有序节点
	currentNode, err = conn.Create(tmpPath, []byte("client"), 3, zk.WorldACL(zk.PermAll))

	// 获取所有列表
	flushActiveList()

	// 选主并运行主节点
	electAndRun()

}

func watchZK(event zk.Event){
	fmt.Println("path:", event.Path)
	fmt.Println("type:", event.Type.String())
	fmt.Println("state:", event.State.String())
}

func watchNodeEvent(e <-chan zk.Event) {
	event := <-e
	fmt.Println("path:", event.Path)
	fmt.Println("type:", event.Type.String())
	fmt.Println("state:", event.State.String())
	// 刷新可用节点列表
	flushActiveList()
	// 选主并运行主节点
	electAndRun()
	// 重新注册监听
	_, _, e, err := conn.ChildrenW(parentPath)
	if err != nil {
		PrintStr("zk parent path query error [%s] when rereigister", err.Error())
	}
	 go watchNodeEvent(e)
}

func flushActiveList() {
	list, _, err := conn.Children(parentPath)
	if err != nil {
		PrintStr("zk create tmp node error %s", err.Error())
		return
	}
	activeList = list
}

func electAndRun() {
	// 获取最小节点
	masterNode = getMinActiveNode(activeList)
	PrintStr("master node is [%s] and currentNode is [%s]", masterNode, currentNode)
	// 是否是主
	isMaster = parentPath+"/"+masterNode == currentNode
	// 如果是主且当前不是运行状态，启动
	lock.Lock()
	defer lock.Unlock()
	if isMaster && !isRunning {
		go doService()
		isRunning = true
	}
}

func doService() {
	fmt.Println(currentNode + " are running")
	if sf != nil{
		fmt.Println("exec sf func")
		sf()
	}
}

func getMinActiveNode(activeNodes []string) string {
	sort.Strings(activeNodes)
	return activeNodes[0]
}

func PrintStr(str string, opts ...interface{}){
	tstr := fmt.Sprintf(str,opts...)
	fmt.Println(tstr)
}
