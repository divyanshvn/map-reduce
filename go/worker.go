package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	coordSockName = sockname

	for {

		// TODO: remove this argument of worker_id, since there is no state of worker, can't store id, and neither is it passed. maybe can randomly generate it though.
		var reply TaskEntry
		err := call("Coordinator.GetTask", EmptyStruct{}, &reply)
		if !err {
			fmt.Println("GetTask failed")
			continue
		}

		var success bool
		var mapResult *MapResult
		var reduceResult *ReduceResult

		switch reply.TaskType {
		case DoneTaskType:
			fmt.Println("Done")
			return

		case WaitTaskType:
			fmt.Println("Wait")
			continue

		case MapTaskType:
			reduceFiles, s := processMapTask(*reply.MapTask, mapf)
			success = s
			mapResult = &MapResult{
				reduceFiles: *reduceFiles,
			}

		case ReduceTaskType:
			finalFile, s := processReduceTask(reply.TaskID, *reply.ReduceTask, reducef)
			success = s
			reduceResult = &ReduceResult{
				finalFile: *finalFile,
			}
		}

		call("Coordinator.ReturnResult", &TaskResult{
			TaskID:       reply.TaskID,
			Success:      success,
			ReduceResult: reduceResult,
			MapResult:    mapResult,
		}, &EmptyStruct{})
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func processMapTask(mapTask MapTask, mapf func(string, string) []KeyValue) (*map[int]string, bool) {
	// reading the file, performing map function on each line
	// and then writing the output of each key (and its values) to a file created on spot, using consistent hashing
	// there will be nReduce number of files created in each map task.

	file, err := os.Open(mapTask.FileName)
	defer file.Close()
	if err != nil {
		return nil, false
	}

	var reduceFiles map[int]string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		keyValArr := mapf(mapTask.FileName, line)

		for i := range len(keyValArr) {
			keyVal := keyValArr[i]

			keyHash := ihash(keyVal.Key)

			fileName, err := reduceFiles[keyHash]
			var file os.File
			if !err {
				fileName := mapTask.FileName + "-" + strconv.Itoa(keyHash)
				newFile, err := os.Create(fileName)
				if err != nil {
					return nil, false
				}

				file = *newFile
				reduceFiles[keyHash] = fileName
			} else {
				newFile, err := os.Open(fileName)
				if err != nil {
					return nil, false
				}

				file = *newFile
			}

			enc, err2 := json.Marshal(keyVal)
			if err2 != nil {
				return nil, false
			}
			file.WriteString(string(enc))
			file.WriteString("\n")
			file.Close()
		}
	}

	return &reduceFiles, true
}

func processReduceTask(taskID uint, reduceTask ReduceTask, reducef func(string, []string) string) (*string, bool) {
	// aggregate files from all map tasks, for this given reduce task.
	// read line by line, deserialize
	files := reduceTask.Files
	keyValueMap := make(map[string][]string)

	for i := range len(files) {
		filename := files[i]
		file, err := os.Open(filename)
		if err != nil {
			return nil, false
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			var keyValue KeyValue
			err := json.Unmarshal([]byte(line), &keyValue)
			if err != nil {
				return nil, false
			}
			_, present := keyValueMap[keyValue.Key]
			if !present {
				keyValueMap[keyValue.Key] = []string{keyValue.Value}
			} else {
				keyValueMap[keyValue.Key] = append(keyValueMap[keyValue.Key], keyValue.Value)
			}
			// 06/14/2026 12:23 AM IST - TODO: add the code to collect files fromm Map tasks in coordinator.
		}
		file.Close()
	}

	outputFileName := "outputFile_" + string(strconv.Itoa(int(taskID))) + ".txt"
	fileOut, err := os.Create(outputFileName)
	defer fileOut.Close()
	if err != nil {
		return nil, false
	}

	for key, values := range keyValueMap {
		// Then, perform the reduce fn on the aggregated values of each key covered in that reduce.
		output := reducef(key, values)
		outStr := key + " " + output + "\n"
		fileOut.WriteString(outStr)
	}

	return &outputFileName, true
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {
	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
