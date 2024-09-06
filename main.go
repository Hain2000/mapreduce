package main

import (
	"fmt"
	singlebasic "mapreduce/single-basic"
)

func main() {
	fmt.Println(singlebasic.IHash("10") % 3)
}