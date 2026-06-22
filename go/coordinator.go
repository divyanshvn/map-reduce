package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type CurrentState int

const (
	MapState CurrentState = iota
	ReduceState
	DoneState
)

type TaskType int

const (
	MapTaskType TaskType = iota
	ReduceTaskType
	WaitTaskType
	DoneTaskType
)

type Coordinator struct {
	// Your definitions here.
	mu                   sync.Mutex
	assignQueue          []TaskEntry
	persistedMapTasks    []MapTask
	persistedReduceTasks []ReduceTask
	assignedMap          map[uint]TaskEntry
	current              CurrentState
	nReduce              int
	nMap                 int
	reduceFileMap        map[int][]string
	finalFileMap         []string
}

type TaskEntry struct {
	TaskID     uint
	TaskType   TaskType
	MapTask    *MapTask
	ReduceTask *ReduceTask
}

type MapTask struct {
	FileName string
	NReduce  int
}

type ReduceTask struct {
	Files []string
}

type TaskResult struct {
	TaskID       uint
	ReduceResult *ReduceResult
	MapResult    *MapResult
	Success      bool
}

type ReduceResult struct {
	finalFile string
}

type MapResult struct {
	reduceFiles map[int]string
}

func (c *Coordinator) GetTask(empty EmptyStruct, task *TaskEntry) error {
	// if the assignQueue is empty AND assignedMap is empty as well AND currentState is Map, then it's time to change CurrentState to Reduce.
	// And fill up assignQueue with nReduce tasks.
	// taskID resets on this transition.
	if len(c.assignQueue) == 0 && len(c.assignedMap) == 0 {
		if c.current == MapState {
			c.current = ReduceState
			for i := 0; i < c.nReduce; i++ {
				files := c.reduceFileMap[i]
				reduceTask := ReduceTask{
					Files: files,
				}
				c.persistedReduceTasks = append(c.persistedReduceTasks, reduceTask)

				taskEntry := TaskEntry{
					TaskID:     uint(i),
					TaskType:   ReduceTaskType,
					MapTask:    nil,
					ReduceTask: &reduceTask,
				}

				c.assignQueue = append(c.assignQueue, taskEntry)
				task = &taskEntry
			}
		} else if c.current == ReduceState {
			// If the currentState is Reduce, and both are empty, then time to be Done and return some Error. (make sure to catch this error in worker)
			c.current = DoneState

			task = &TaskEntry{
				TaskID:     0,
				TaskType:   DoneTaskType,
				MapTask:    nil,
				ReduceTask: nil,
			}
			return nil
		} else {
			return errors.New("invalid state")
		}
	}

	// if the assignedMap isn't empty but assignQueue is empty, then might be a good idea to assign an unfinished task to another worker.
	if len(c.assignedMap) != 0 && len(c.assignQueue) == 0 {
		task = &TaskEntry{
			TaskID:     0,
			TaskType:   WaitTaskType,
			MapTask:    nil,
			ReduceTask: nil,
		}
		return nil
	}

	// w.r.t. currentState, assign new task to the worker and populate assignedMap.
	newTask := c.assignQueue[0]
	c.assignQueue = c.assignQueue[1:]

	c.assignedMap[newTask.TaskID] = newTask
	task = &newTask

	return nil
}

type EmptyStruct struct{}

func (c *Coordinator) ReturnResult(taskResult *TaskResult, reply *EmptyStruct) error {
	// make sure to handle for the result whose task is already finished by another worker.
	taskID := taskResult.TaskID
	if !taskResult.Success {
		// re-assign the task on another GetTask request
		c.assignQueue = append(c.assignQueue, c.assignedMap[taskID])
		delete(c.assignedMap, taskID)
		return nil
	}

	if c.current == MapState {
		reduceFiles := taskResult.MapResult.reduceFiles
		for reduceNum, fileName := range reduceFiles {
			c.reduceFileMap[reduceNum] = append(c.reduceFileMap[reduceNum], fileName)
		}
	} else if c.current == ReduceState {
		finalFile := taskResult.ReduceResult.finalFile
		c.finalFileMap[taskResult.TaskID] = finalFile
	}
	// Take in the result from the worker, first remove the entry from assignedMap.
	delete(c.assignedMap, taskID)

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	if c.current == DoneState {
		return true
	}
	// checks if currentState is Reduce and the assignQueue is empty too.
	if c.current == ReduceState && len(c.assignQueue) == 0 && len(c.assignedMap) == 0 {
		c.current = DoneState
		return true
	}

	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	nMap := len(files)
	assignQueue := make([]TaskEntry, 0)
	mapTasks := make([]MapTask, 0)

	for i := 0; i < nMap; i++ {
		fileName := files[i]

		mapTask := MapTask{
			FileName: fileName,
			NReduce:  nReduce,
		}
		mapTasks = append(mapTasks, mapTask)

		assignQueue = append(assignQueue, TaskEntry{
			TaskID:     uint(i),
			ReduceTask: nil,
			MapTask:    &mapTask,
		})
	}
	c := Coordinator{
		assignQueue:          assignQueue,
		assignedMap:          make(map[uint]TaskEntry),
		nMap:                 nMap,
		nReduce:              nReduce,
		current:              MapState,
		reduceFileMap:        make(map[int][]string, nReduce),
		persistedMapTasks:    mapTasks,
		persistedReduceTasks: make([]ReduceTask, 0),
		finalFileMap:         make([]string, nReduce),
	}

	c.server(sockname)
	return &c
}
