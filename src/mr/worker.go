package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// 求数组a的长度
func (a ByKey) Len() int {
	return len(a)
}
// 交换数组下表i, j 对应的两个元素
func (a ByKey) Swap(i, j int)  {
	a[i], a[j] = a[j], a[i]
}

// 排序的比较函数
func (a ByKey) Less(i, j int)  bool {
	return a[i].Key < a[j].Key
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
/*
实现worke功能的函数
worker向master请求执行哪个work(map or reduce)
若 map任务被另一个worker接受但还没有完成, 这个worker会等一会(等待接受map任务);
若 map任务被另一个worker完成, master会提供必要reduce任务信息给这个worker(接受reduce任务)
若 所有的reduce任务完成了, 输入enter 退出程序
 */

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		taskInfo := CallAskTask()
		// 根据worker请求相应的master答复, worker的操作
		// 分别有 map任务, reduce任务, 当前可能还有新任务, 任务全部完成 以及 无效任务
		switch taskInfo.State {
		case TaskMap:
			workerMap(mapf, taskInfo)
			break
		case TaskReduce:
			workerReduce(reducef, taskInfo)
			break
		case TaskWait:
			// 等待5秒后再次发起请求
			time.Sleep(time.Duration(time.Second * 5))
			break
		case TaskEnd:
			fmt.Println("Master all tasks complete. Nothing to do...")
			// 退出worker进程
			return
		default: // worker接受到的任务无效
			panic("Invalid Task state recevied by worker")
		}
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

// worker发出任务请求, 返回任务答复信息
func CallAskTask() *TaskInfo  {
	args := ExampleArgs{}
	reply := TaskInfo{}  //答复
	call("Master.AskTask", &args, &reply)
	return &reply
}

func CallTaskDone(taskinfo *TaskInfo)  {
	reply := ExampleReply{}
	call("Master.TaskDone", taskinfo, &reply)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// worker执行map任务

func workerMap(mapf func(string, string) []KeyValue, taskInfo *TaskInfo)  {
	fmt.Printf("Got assigned map task on %vth file %v\n", taskInfo.FileIndex, taskInfo.FileName)

	// 以键值数组的形式读取目标文件
	intermediate := []KeyValue{}
	file, err := os.Open(taskInfo.FileName)
	if err != nil {
		log.Fatal("cannot open %v", taskInfo.FileName)
	}
	content, err := ioutil.ReadAll(file) // 读取文件全部内容直到结束
	if err != nil {
		log.Fatal("cannot read %v", taskInfo.FileName)
	}
	file.Close()
	kva := mapf(taskInfo.FileName, string(content)) // 调用map函数
	intermediate = append(intermediate, kva...)  // 追加map函数生成的中间文件

	// 准备输出文件和编码器
	nReduce := taskInfo.NReduce
	// 输出文件前缀
	// map任务生成的中间文件名为 mr-fileIndex-
	outprefix := "mr-tmp/mr-"
	outprefix += strconv.Itoa(taskInfo.FileIndex) // 连接文件索引
	outprefix += "-"
	outFiles := make([] *os.File, nReduce)  //动态数组, 大小为nReduce
	fileEncs := make([] *json.Encoder, nReduce) // json.Encoder 即为将文件编码为json格式的编码器 fileEncs
	// 还有reduce任务
	for outindex := 0; outindex < nReduce; outindex ++ {
		// outFiles[outindex]为 创建的临时文件
		// 创建临时文件 是可能mapf或reducef进程由于某些原因退出而产生不完整文件
		// reduce任务无法分清哪些是成功的map任务输出的,所以先用临时文件暂存,等待所有任务成功完成后重命名为mr-m-n
		outFiles[outindex], _ = ioutil.TempFile("mr-tmp", "mr-tmp-*")
		// 将outFiles[outindex]写入返回的编码器为fileEncs[outindex]
		fileEncs[outindex] = json.NewEncoder(outFiles[outindex])
	}

	// distribute keys among mr-fileindex-*
	// 将intermediate数组的KeyValue对象写入到这些输出文件中
	for _, kv := range intermediate {
		outindex := ihash(kv.Key) % nReduce
		file = outFiles[outindex]
		enc := fileEncs[outindex]
		err := enc.Encode(&kv)  // 编码为json文件
		if err != nil {
			fmt.Printf("File % Key %v Value %v Error: %v\n", taskInfo.FileName, kv.Key)
			panic("Json encode failed")
		}
	}

	// 保存文件
	// outindex 为临时文件outFiles的索引, 最终map任务的文件名为outname: mr-m-n, m表示第m个map任务, n表示第n个reduce任务
	for outindex, file := range outFiles {
		outname := outprefix + strconv.Itoa(outindex)
		oldpath := filepath.Join(file.Name()) // 旧的文件路径
		// 文件重命名为outname
		os.Rename(oldpath, outname)
		file.Close()
	}
	// 通知master 完成了map任务
	CallTaskDone(taskInfo)
}

// worker执行reduce任务

func workerReduce(reducef func(string, []string) string, taskInfo *TaskInfo)  {
	fmt.Printf("Got assigned reduce task on part %v\n", taskInfo.PartIndex)
	// 执行完reduce任务后的文件名为outname mr-out-n
	outname := "mr-out-" + strconv.Itoa(taskInfo.PartIndex)
	// fmt.Printf("%v\n", taskInfo)

	// 读取map任务的输出文件
	innameprefix := "mr-tmp/mr-"
	innamesuffix := "-" + strconv.Itoa(taskInfo.PartIndex)

	// read in all files as a key array
	// 中间文件为 键值对的数组形式
	intermediate := []KeyValue{}
	for index := 0; index < taskInfo.NFiles; index ++ {
		inname := innameprefix + strconv.Itoa(index) + innamesuffix
		file, err := os.Open(inname)
		if err != nil {
			fmt.Printf("Open intermediate file %v failed: %v\n", inname, err)
			panic("Open file error")
		}
		// 返回读file后的解码器: dec
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			// 文件解码
			if err := dec.Decode(&kv); err != nil {
				//fmt.Printf("%v\n", err)
				break
			}
			//fmt.Printf("%v\n", kv)
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))  // 中间文件按键排序

	ofile, err := ioutil.TempFile("mr-tmp", "mr-*")
	if err != nil {
		fmt.Printf("Create output file %v failed: %v\n", outname, err)
		panic("Create file error")
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		// 找出所有中间文件相同的key
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j ++
		}
		values := []string{}
		// 相同key追加j-i次
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// 执行reduce函数后的输出为output
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		// 将最终的reduce任务执行完毕后的output写入ofile
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j // 下一轮循环开始的位置
	}
	// filepath.Join() 将任意数量的路径元素加入到单一路径中, os.Rename()重命名为新路径即outname
	os.Rename(filepath.Join(ofile.Name()), outname)
	ofile.Close()
	// 通知master 完成了reduce 任务
	CallTaskDone(taskInfo)

}