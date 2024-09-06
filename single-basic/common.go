package singlebasic

import (
	"hash/fnv"
	"strconv"
)

type jobPhase string

const (
	mapPhase jobPhase = "Map"
	reducePhase jobPhase = "Reduce"
)

type KeyValue struct {
	Key string
	Value string
}

func mergeName(jobName string, reduceTask int) string {
	return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}

func reduceName(jobNmae string, mapTask, reduceTask int) string {
	return "mrtmp." + jobNmae + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

func IHash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}