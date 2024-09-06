package singlebasic

import "fmt"

func Seqiemtial(jobName string, files []string, nReduce int, mapF func(string, string) []KeyValue, reduceF func(string, []string) string) {
	mr := newMaster()
	mr.run(jobName, files, nReduce, func(phase jobPhase) {
		switch phase {
		case mapPhase:
			for i, f := range files {
				doMap(jobName, i, f, nReduce, mapF)
			}
			
		case reducePhase:
			for i := 0; i < nReduce; i++ {
				doReduce(jobName, i, mergeName(jobName, i), len(files), reduceF)
			}

		}

	})
	
	
}

type Master struct {

}

func newMaster() *Master {
	return &Master{}
}

func (mr *Master) run(jobName string, files []string, nReduce int, schedule func(phase jobPhase)) {
	schedule(mapPhase)
	schedule(reducePhase)
	mr.merge(nReduce, jobName)
	fmt.Println("OK")
}