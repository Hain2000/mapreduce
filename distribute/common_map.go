package distribute

import (
	"encoding/json"
	"io"
	"os"

	"github.com/labstack/gommon/log"
)

// 将输出分成指定数量的中间文件, mapTaskNumber 当前map任务编号， nReduce 当前任务执行的reduce编号
func doMap(jobName string, mapTaskNumber int, inFile string, nReduce int, mapF func(file, contents string) []KeyValue) {
	f , err := os.Open(inFile)
	if err != nil {
		log.Errorf("open file %s failed! %v\n", inFile,  err)
	}
	defer f.Close()

	content, err := io.ReadAll(f)
	if err != nil {
		log.Errorf("read the content of file failed! %v\n", err)
	}

	kvs := mapF(inFile, string(content))
	encoders := make([]*json.Encoder, nReduce)

	for i := 0; i < nReduce; i++ {
		file_name := reduceName(jobName, mapTaskNumber, i)
		f, err := os.Create(file_name)
		if err != nil {
			log.Errorf("unable to create file [%s]: %v\n", file_name, err)
		}
		defer f.Close()
		encoders[i] = json.NewEncoder(f)
	}

	for _, v := range kvs {
		// 自定义规则对key值进行分类
		// 编号哈希值对nReduce取余进行分类
		index := IHash(v.Key) % nReduce
		if err := encoders[index].Encode(&v); err != nil {
			log.Errorf("encode err: %v\n", err)
		}
	}
}

