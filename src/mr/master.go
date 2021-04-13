package mr

import (
	"fmt"
	"sync"
	"time"
)
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

// 构造TaskStat即任务结构体

type TaskStat struct {
	beginTime time.Time
	fileName string
	fileIndex int
	partIndex int //
	nReduce   int // Reduce任务数量为n
	nFiles 	  int
}

// 定义任务类接口, 两种任务 map 和 reduce分别实现该接口

type TaskStatInterface interface {
	GenerateTaskInfo() TaskInfo
	OutOfTime() bool	//是否超时的标志
	GetFileIndex() int
	GetPartIndex() int
	SetNow() 
}

// MapTaskStat 和 下面的ReduceTaskStat都继承了TaskStat
// 在继承者的struct中写被继承者的名称 来实现继承

type MapTaskStat struct {
	// Your definitions here.
	TaskStat
}

type ReduceTaskStat struct {
	TaskStat
}

// 下面的几个函数实现了TaskStatInterface接口
// 函数GenerateTaskInfo，让从MapTaskStat或ReduceTaskStat向TaskInfo的转化非常方便

func (this *MapTaskStat) GenerateTaskInfo() TaskInfo {
	return TaskInfo{
		State: TaskMap,
		FileName: this.fileName,
		FileIndex: this.fileIndex,
		PartIndex: this.partIndex,
		NReduce: this.nReduce,
		NFiles: this.nFiles,
	}
}

func (this *ReduceTaskStat) GenerateTaskInfo() TaskInfo  {
	return TaskInfo{
		State: TaskReduce,
		FileName: this.fileName,
		FileIndex: this.fileIndex,
		PartIndex: this.partIndex,
		NReduce: this.nReduce,
		NFiles: this.nFiles,
	}
}

// 任务超时判断函数

func (this *TaskStat) OutOfTime() bool {
	// 若任务执行时间超过60s 则视为任务超时
	return time.Now().Sub(this.beginTime) > time.Duration(time.Second * 60)
}

func (this *TaskStat) GetFileIndex() int  {
	return this.fileIndex
}

func (this *TaskStat) GetPartIndex() int {
	return this.partIndex
}

func (this *TaskStat) SetNow()  {
	this.beginTime = time.Now()
}


type TaskStatQueue struct {
	taskArray []TaskStatInterface //任务队列数组(原作者的写法实际是后进先出, 栈的方式)
	mutex  	sync.Mutex  //同步锁
}

// 队列加锁
func (this *TaskStatQueue) lock()  {
	this.mutex.Lock()
}

// 开锁
func (this *TaskStatQueue) unlock()  {
	this.mutex.Unlock()
}

func (this *TaskStatQueue) Size() int  {
	return len(this.taskArray)
}

// 执行任务前都应对其进行加锁, 任务执行完毕后释放锁

func (this *TaskStatQueue) Pop()  TaskStatInterface {
	this.lock()
	arrayLength := len(this.taskArray)  // 求任务队列(数组)长度
	if arrayLength == 0 { // 没有任务了
		this.unlock()
		return nil
	}
	ret := this.taskArray[arrayLength-1]  // 取出队尾任务
	this.taskArray = this.taskArray[:arrayLength-1]
	this.unlock()
	return ret
}

// 入队

func (this *TaskStatQueue) Push(taskStat TaskStatInterface)  {
	this.lock()
	if taskStat == nil {
		this.unlock()
		return
	}
	this.taskArray = append(this.taskArray, taskStat)
	this.unlock()
}

// 任务超时队列

func (this *TaskStatQueue) TimeOutQueue() []TaskStatInterface {
	outArray := make([]TaskStatInterface, 0)
	this.lock()
	for taskIndex := 0; taskIndex < len(this.taskArray); {
		taskStat := this.taskArray[taskIndex]
		if (taskStat).OutOfTime() {
			// 当前任务taskStat超时
			outArray = append(outArray, taskStat)
			this.taskArray = append(this.taskArray[:taskIndex], this.taskArray[taskIndex+1:]...)
			// must resume at this index next time ??
		} else {
			taskIndex++
		}
	}
	this.unlock()
	return outArray
}

// ???

func (this *TaskStatQueue) MoveAppend(rhs []TaskStatInterface)  {
	this.lock()
	this.taskArray = append(this.taskArray, rhs...)
	rhs = make([]TaskStatInterface, 0)
	this.unlock()
}

// 移除任务

func (this *TaskStatQueue) RemoveTask(fileIndex int, partIndex int)  {
	this.lock()
	for index := 0; index < len(this.taskArray); {
		task := this.taskArray[index]
		if fileIndex == task.GetFileIndex() && partIndex == task.GetPartIndex() {
			this.taskArray = append(this.taskArray[:index], this.taskArray[index+1:]...)
		} else {
			index ++
		}
	}
	this.unlock()
}

type Master struct {
	filenames []string

	// reduce 任务队列
	reduceTaskWaiting TaskStatQueue
	reduceTaskRunning TaskStatQueue

	// map 任务队列
	mapTaskWaiting TaskStatQueue
	mapTaskRunning TaskStatQueue

	// machine state
	isDone bool
	// reduce 任务数量
	nReduce int
}


// Your code here -- RPC handlers for the worker to call.
// 处理来worker 的任务请求

func (this *Master) AskTask(args *ExampleArgs, reply *TaskInfo) error  {
	if this.isDone {
		reply.State = TaskEnd
		return nil
	}
	// 存在没有完成的任务(这里说的任务指 map 和/或 reduce)
	// 检查reduce任务
	reduceTask := this.reduceTaskWaiting.Pop()
	if reduceTask != nil {
		// reduceTask非空, 则存在一个未分配的reduce任务, 记录任务开始时间
		reduceTask.SetNow()
		// 任务正在执行
		this.reduceTaskRunning.Push(reduceTask)
		// 答复
		*reply = reduceTask.GenerateTaskInfo()
		fmt.Printf("Distributing reduce task on part %v %vth file %v\n", reply.PartIndex, reply.FileIndex, reply.FileName)
		return nil
	}

	// 检查map任务
	mapTask := this.mapTaskWaiting.Pop()
	if mapTask != nil {
		// 存在一个未分配的map任务, 记录开始时间
		mapTask.SetNow()
		// 任务正在执行
		this.mapTaskRunning.Push(mapTask)
		// 答复
		*reply = mapTask.GenerateTaskInfo()
		fmt.Printf("Distributing map task on %vth file %v\n", reply.FileIndex, reply.FileName)
		return nil
	}

	// 分配完所有任务 且 还有map或reduce任务未完成,则可能有新的任务产生
	if this.mapTaskRunning.Size() > 0 || this.reduceTaskRunning.Size() > 0 {
		// 必须等待新的任务
		reply.State = TaskWait
		return nil
	}

	// 完成了所有任务
	reply.State = TaskEnd
	this.isDone = true
	return nil

}

// 分配reduce任务
func (this *Master) distributeReduce()  {
	reduceTask := ReduceTaskStat{
		TaskStat{
			fileIndex: 0,
			partIndex: 0,
			nReduce: this.nReduce,
			nFiles: len(this.filenames),
		},
	}
	for reduceindex := 0; reduceindex < this.nReduce; reduceindex ++ {
		task := reduceTask
		task.partIndex = reduceindex // reduce任务的索引
		this.reduceTaskWaiting.Push(&task) // 将task加入reduce任务等待执行队列
	}
}

// 任务完成
// 完成的任务可能是map 或 reduce, 将完成的任务从队列中移出
// 若完成的是map任务 还需判断分配的map任务全部完成, 若是则分配reduce任务; 若是reduce 移除就可

func (this *Master) TaskDone(args *TaskInfo, reply *ExampleReply) error  {
	// master根据worker的答复中任务状态执行对应的操作
	switch args.State {
	case TaskMap:
		// map任务完成, 从进行中map任务队列移除该map任务(根据Fileindex 和 PartIndex定位)
		fmt.Printf("Map task on %vth file %v complete\n", args.FileIndex, args.FileName)
		this.mapTaskRunning.RemoveTask(args.FileIndex, args.PartIndex)
		if this.mapTaskRunning.Size() == 0 && this.mapTaskWaiting.Size() == 0 {
			// 分配的map任务全部完成了, master给worker分配新的reduce任务
			this.distributeReduce()
		}
		break
	case TaskReduce:
		fmt.Printf("Reduce task on %vth part complete\n", args.PartIndex)
		this.reduceTaskRunning.RemoveTask(args.FileIndex, args.PartIndex)
		break
	default:
		panic("Task Done error")
	}
	return nil
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func MakeMaster(files []string, nReduce int) *Master {
	// 分配map任务
	mapArray := make([]TaskStatInterface, 0)
	for fileIndex, filename := range files {
		mapTask := MapTaskStat{
			TaskStat{
				fileName: filename,
				fileIndex: fileIndex,
				partIndex: 0,
				nReduce: nReduce,
				nFiles: len(files),
			},
		}
		mapArray = append(mapArray, &mapTask)
	}
	m := Master{
		mapTaskWaiting: TaskStatQueue{taskArray: mapArray},
		nReduce: nReduce,
		filenames: files,
	}

	// 若不存在临时文件夹则创建
	if _, err := os.Stat("mr-tmp"); os.IsNotExist(err) {
		err = os.Mkdir("mr-tmp", os.ModePerm)  // 创建名为mr-tmp的文件夹
		if err != nil {
			fmt.Print("Create tmp directory failed... Error: %v\n", err)
			panic("Create tmp directory failed...")
		}
	}

	// 为收集超时任务开启一个线程
	go m.collectOutOfTime()
	// Your code here.

	m.server()
	return &m
}

func (this *Master) collectOutOfTime()  {
	for{
		time.Sleep(time.Duration(time.Second * 5))
		timeouts := this.reduceTaskRunning.TimeOutQueue() // reduce任务超时队列
		if len(timeouts) > 0{
			this.reduceTaskWaiting.MoveAppend(timeouts)
		}
		timeouts = this.mapTaskRunning.TimeOutQueue() // map任务超时队列
		if len(timeouts) > 0 {
			this.mapTaskWaiting.MoveAppend(timeouts)
		}
	}
}
