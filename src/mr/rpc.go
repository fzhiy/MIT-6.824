package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TryMapArgs struct {

}

type TryMapReply struct {
	// 如果不应该执行map任务, 则执行reduce任务
	RunMap bool
}


const (
	TaskMap = 0
	TaskReduce = 1
	TaskWait = 2
	TaskEnd = 3
)

type TaskInfo struct {
	State int // 一共四种状态
	/*
	 0 map
	 1 reduce
	 2 wait
	 3 end
	*/

	FileName string // 文件名称
	FileIndex int   // map任务的索引
	PartIndex int   // reduce任务的索引
	NReduce int		// reduce任务的个数
	NFiles int		// 一个map任务需要处理的文件个数
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
