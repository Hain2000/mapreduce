package distribute

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
)

func (mr *Master) startPRCServer() {
	rpcs := rpc.NewServer()
	rpcs.Register(mr)
	os.Remove(mr.address)
	listener, err := net.Listen("unix", mr.address)
	if err != nil {
		log.Fatalf("RegisterServer %s error: %v!\n", mr.address, err)
	}
	mr.listener = listener
	go func() {
	loop:
		for {
			// 检测是否接收到中断
			select {
			case <-mr.shutdown:
				break loop
			default:

			}
			conn, err := mr.listener.Accept() // 等待RPC连接
			if err != nil {
				log.Fatalf("RegisterServer: accept error: %v \n", err)
				break
			} else {
				// ???
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			}
		}
		fmt.Println("RegisterServer: done!")
	}()
}

// 紧急中断
func (mr *Master) ShutDown(_, _ *struct{}) error {
	log.Fatalf("Shutdown: registration server\n")
	close(mr.shutdown)
	mr.listener.Close()
	return nil
}

func (mr *Master) stopRPCServer() {

}
