package singlebasic

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

// 将多个reduce结点生成的最终结果输出文件输出到一个文件中进行排序，汇总
func (mr *Master) merge(nReduce int, jobName string) {
	fmt.Println("Merge result...")
	kvs := make(map[string]string)
	for i := 0; i < nReduce; i++ {
		p := mergeName(jobName, i)
		file, err := os.Open(p)
		if err != nil {
			log.Panicf("Merge : %v failed %v\n", p, err)
		}

		decoder := json.NewDecoder(file)
		
		for ; decoder.More(); {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err != nil {
				log.Panicf("Json decode failed! %v", err)
			}
			kvs[kv.Key] = kv.Value
		}

		file.Close()
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	file, err := os.Create("mrtmp." + jobName)
	if err != nil {
		log.Panicf("Merge create failed %v\n", err)
	}

	w := bufio.NewWriter(file)
	for _, k := range keys {
		fmt.Fprintf(w, "%s: %s\n", k, kvs[k])
	}
	w.Flush()
	file.Close()
}