package test

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"sort"
	"sync"
	"time"
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



// 运行锁，防止程序运行两遍
var lock sync.Mutex

var sf func()


type Config struct {
	ZkList []string `yaml:"zkList"`
}

func initConfig() {
	configByte, err := ioutil.ReadFile("etc\\config.yml")
	if err != nil {
		fmt.Println(err)
	}
	config = Config{}
	yaml.Unmarshal(configByte, &config)
	fmt.Println("zk list ", config.ZkList)
}

func InitZK(f func()) {
	//初始zk配置
	initConfig()
	// 业务调用方法
	sf = f
	//  注册zookeeper
	c, _, err := zk.Connect(config.ZkList, 15*time.Second)
	conn = c
	if err != nil {
		fmt.Println("zk connect error")
		return
	}
	isExist, _, e, err := conn.ExistsW(parentPath)
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

}

func watchNodeEvent(e <-chan zk.Event) {
	event := <-e
	fmt.Println("path:", event.Path)
	fmt.Println("type:", event.Type.String())
	fmt.Println("state:", event.State.String())
	if "EventNodeChildrenChanged" == event.Type.String(){
		// 刷新可用节点列表
		flushActiveList()
		// 选主并运行主节点
		electAndRun()
	}
	// 重新注册监听
	_, _, e, err := conn.ExistsW(parentPath)
	if err != nil {
		println("zk parent path query error [%s] when rereigister", err.Error())
	}
	go watchNodeEvent(e)
}

func flushActiveList() {
	lock.Lock()
	defer lock.Unlock()
	list, _, err := conn.Children(parentPath)
	if err != nil {
		println("zk lo tmp node error %s", err.Error())
		return
	}
	activeList = list
}

func electAndRun() {
	lock.Lock()
	defer lock.Unlock()
	// 获取最小节点
	masterNode = getMinActiveNode(activeList)
	fmt.Println("master node is ", masterNode,"and current node is ",currentNode)
	// 是否是主
	isMaster = parentPath+"/"+masterNode == currentNode
	// 如果是主且当前不是运行状态，启动
	if isMaster && !isRunning {
		go doService()
		isRunning = true
	}
}

func doService() {
	fmt.Println(currentNode + " are running")
	sf()
}

func getMinActiveNode(activeNodes []string) string {
	sort.Strings(activeNodes)
	return activeNodes[0]
}

func testGetMinActiveNode() {
	var activeNodes = []string{"app00002", "app00001"}
	fmt.Println(getMinActiveNode(activeNodes))
}
