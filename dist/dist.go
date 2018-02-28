package dist

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"sort"
	"sync"
	"time"
	"pkg/errors"
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

// 业务开始方法
var sfStartFunc func()

// 业务结束方法
var sfEndFunc func()

// 配置文件路径
var configPath string = "etc/config.yml"


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

func InitZK(fStartFunc func(), fEndFunc func(), configFilePath string) {
	if configFilePath != ""{
		configPath = configFilePath
	}
	//初始zk配置
	initConfig()
	// 初始化业务方法
	sfStartFunc = fStartFunc
	sfEndFunc = fEndFunc

	// 连接并监听
	connectZKAndListen()

	// 创建持久化节点
	createPersistParentNode()

	// 创建临时节点并注册监听
	createAndListenTmpNode()

	// 获取所有列表
	flushActiveList()

	// 选主并运行主节点
	electAndRun()

}

// 连接并监听zookeeper
func connectZKAndListen(){
	// 与zookeeper回调
	zkCallBack := zk.WithEventCallback(watchZK)
	//  注册zookeeper
	c, _, err := zk.Connect(config.ZkList, 15*time.Second,zkCallBack)
	conn = c
	if err != nil {
		fmt.Println("zk connect error")
		panic(errors.New("can't connect zk cluster"))
	}
}

// 创建持久化父节点
func createPersistParentNode(){
	isExist, _, _, err := conn.ExistsW(parentPath)
	if err != nil {
		PrintStr("zk parent path query error [%s]", err.Error())
		panic(errors.New("can't query zk nodes"))
	}
	if !isExist {
		// 创建持久化节点
		_, err := conn.Create(parentPath, []byte("async"), 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			PrintStr("zk crate parent [%s] path error [%s]", parentPath, err.Error())
			panic(errors.New("can't create zk persistent node"))
		}
	}
}

// 创建临时节点并注册监听
func createAndListenTmpNode(){
	// 注册监听
	_, _, e, err := conn.ChildrenW(parentPath)
	go watchNodeEvent(e)
	fmt.Println("register node event watching")
	// 创建临时有序节点
	currentNode, err = conn.Create(tmpPath, []byte("client"), 3, zk.WorldACL(zk.PermAll))
	if err != nil{
		PrintStr("zk crate tmp node and parent path is [%s] error [%s]", parentPath, err.Error())
		panic(errors.New("can't create zk tmp node"))
	}
}

// 失联后停止服务,全局监听内不能使用conn的任何方法，否则报死锁
func watchZK(event zk.Event){
	fmt.Println("zk path:", event.Path)
	fmt.Println("zk type:", event.Type.String())
	fmt.Println("zk state:", event.State.String())
	state := event.State.String();
	eventType := event.Type.String();
	if "EventSession" == eventType && "StateDisconnected" == state{
		stopService()
	}
}

//  如果节点发生变化，更新可用列表， 重新选举，
//  如果当期节点服务正在运行，能收到该事件说明该节点肯定没死掉，主节点不会发生改变
//  如果当前节点服务没有运行，收到后可能重新成为主节点，运行服务
//  唯一发生安全问题的就是该方法，可能同时触发多次
func watchNodeEvent(e <-chan zk.Event) {
	event := <-e
	fmt.Println("node path:", event.Path)
	fmt.Println("node type:", event.Type.String())
	fmt.Println("node state:", event.State.String())
	if "EventNodeChildrenChanged" == event.Type.String(){
		// 刷新可用节点列表
		flushActiveList()
		// 选主并运行主节点
		electAndRun()
	}
	// 重新注册监听
	_, _, e, err := conn.ChildrenW(parentPath)
	if err != nil {
		PrintStr("zk parent path query error [%s] when rereigister", err.Error())
	}
	 go watchNodeEvent(e)
}

// 更新可用节点，如果长时间失联，zk一直心跳，当前节点再重新连接上,之前的节点已经不在
func flushActiveList() {
	lock.Lock()
	defer lock.Unlock()
	list, _, err := conn.Children(parentPath)
	if err != nil {
		PrintStr("zk create tmp node error %s", err.Error())
	}
	activeList = list
	//state := conn.State()
	fmt.Println(fmt.Sprintf("active nodes is [%v] ",activeList))
}

// 选举并运行要启动的服务
func electAndRun() {
	lock.Lock()
	defer lock.Unlock()
	// 获取最小节点
	masterNode = getMinActiveNode(activeList)
	PrintStr("master node is [%s] and currentNode is [%s]", masterNode, currentNode)
	// 是否是主
	isMaster = parentPath+"/"+masterNode == currentNode
	// 如果是主且当前不是运行状态，启动
	if isMaster && !isRunning {
		PrintStr("currentNode [%s] do service",currentNode)
		go doService()
		isRunning = true
	}
}

// 启动服务
func doService() {
	fmt.Println(currentNode + " are running")
	if sfStartFunc != nil{
		fmt.Println("exec sf func")
		sfStartFunc()
	}
}

// 停止服务
func stopService(){
	fmt.Println(currentNode + "will stop")
	lock.Lock()
	defer lock.Unlock()
	isMaster = false
	activeList = nil
	isRunning = false
	if sfEndFunc != nil{
		fmt.Println("exce stop func")
		sfEndFunc()
	}
}

// 获取最小节点，该节点作为主节点
func getMinActiveNode(activeNodes []string) string {
	if activeNodes == nil || len(activeNodes) == 0 {
		return ""
	}
	sort.Strings(activeNodes)
	return activeNodes[0]
}

func PrintStr(str string, opts ...interface{}){
	tstr := fmt.Sprintf(str,opts...)
	fmt.Println(tstr)
}
