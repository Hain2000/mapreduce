package distribute

import (
	"bufio"
	"fmt"
	"github.com/labstack/gommon/log"
	"os"
	"strings"
	"testing"
)

const (
	nNumber = 100
)


func MapFunc(file, value string) (res []KeyValue) {
	words := strings.Fields(value)
	for _, w := range words {
		kv := KeyValue{w, ""}
		res = append(res, kv)
	}
	return
}


func ReduceFunc(key string, values []string) string {
	for _, v := range values {
		fmt.Printf("Reduce %s-%v\n", key, v)
	}
	return ""
}

func TestSequentialSignle(t *testing.T) {
	Seqiemtial("test", makeInputs(1), 1, MapFunc, ReduceFunc)
}

func TestSequentialMany(t *testing.T) { 
	Seqiemtial("test", makeInputs(5), 3, MapFunc, ReduceFunc) // 有 5 * 3 个中间结果集
}

func makeInputs(n int) []string {
	var names []string
 	i := 0
	for f := 0; f < n; f++ {
		names = append(names, fmt.Sprintf("824-mrinput-%d.txt", f))
		file, err := os.Create(names[f])
		if err != nil {
			log.Errorf("create input file [%s] failed. error:", file, err)
		}
		w := bufio.NewWriter(file)
		for i < (f + 1) * (nNumber / n) {
			fmt.Fprintf(w, "%d\n", i)
			i++
		}
		w.Flush() // 把buf里的东西刷盘
		file.Close()
	}
	return names
}