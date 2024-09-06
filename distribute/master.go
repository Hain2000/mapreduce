package distribute

import (
	"fmt"
	"net"
	"sync"


)

type Master struct {
	address  string
	mtx      sync.Mutex
	workers  []string // 存储套接字，也就是代表RPC地址
	jobName  string   // 当前要执行的任务名称
	files    []string // 输入文件
	nReduce  int      // reduce分区数量
	listener net.Listener // 监听对象
	shutdown chan struct{}  // 中断服务
}

func newMaster(master string) *Master {
	mr := new(Master)
	mr.address = master
	mr.shutdown = make(chan struct{})
	return mr
}

func (mr *Master) run(jobName string, files []string, nReduce int, schedule func(phase jobPhase)) {
	schedule(mapPhase)
	schedule(reducePhase)
	mr.jobName = jobName
	mr.files = files
	mr.nReduce = nReduce
	mr.merge()
	fmt.Println("OK")
}

func Seqiemtial(jobName string, files []string, nReduce int, mapF func(string, string) []KeyValue, reduceF func(string, []string) string) {
	mr := newMaster("master")
	mr.run(jobName, files, nReduce, func(phase jobPhase) {
		switch phase {
		case mapPhase:
			for i, f := range files {
				doMap(mr.address, i, f, mr.nReduce, mapF)
			}

		case reducePhase:
			for i := 0; i < mr.nReduce; i++ {
				doReduce(mr.jobName, i, mergeName(mr.jobName, i), len(files), reduceF)
			}

		}

	})

}
