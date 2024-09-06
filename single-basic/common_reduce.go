package singlebasic

import (
	"encoding/json"
	"os"

	"github.com/labstack/gommon/log"
)


func doReduce(jobName string, reduceTaskNumber int, outFile string, nMap int, reduceF func(string, []string) string) {

	res := make(map[string][]string)

	for i := 0; i < nMap; i++ {
		interFile := reduceName(jobName, i, reduceTaskNumber)
		f, err := os.Open(interFile)
		if err != nil {
			log.Errorf("read contenct from file [%s] failed! %v\n", interFile, err)
		}
		defer f.Close()

		decoder := json.NewDecoder(f)
		var kv KeyValue
		for ; decoder.More(); {
			if err := decoder.Decode(&kv); err != nil {
				log.Errorf("Json decode faild %v\n", err)
			}
			res[kv.Key] = append(res[kv.Key], kv.Value)
		}
	}

	out_file, err := os.Create(outFile)
	if err != nil {
		log.Errorf("create outFile failed %v", err)
	}
	defer out_file.Close()

	encoder := json.NewEncoder(out_file)
	for k := range res {
		encoder.Encode(KeyValue{Key: k, Value: reduceF(k, res[k])})
	}
}