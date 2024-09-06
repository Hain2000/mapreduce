package distribute

import "net/rpc"


type ShutdonwReply struct {
	Ntasks int // 代表指定worker执行的当前为止的任务数量
}


func call(srv, rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		return false
	}
	defer c.Close()
	if err = c.Call(rpcname, args, reply); err != nil {
		return false
	}
	return true
}