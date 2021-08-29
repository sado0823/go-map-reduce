package syncx

import (
	"fmt"
	"github.com/sado0823/go-map-reduce/internal/size"
	"log"
	"runtime"
	"strings"
)

func Go(fn func()) {
	go func() {
		defer func() {
			// recover this unmanageable func
			if r := recover(); r != nil {
				log.Printf("r:%v, \n stack:%s", r, printStack())
				fmt.Printf("r:%v, \n stack:%s", r, printStack())
			}
		}()
		// execute this goroutine func
		fn()
	}()
}

func printStack() []string {
	//buf := make([]byte, 64<<10)
	buf := make([]byte, size.KB.Uint64()*64)
	n := runtime.Stack(buf, false)
	buf = buf[:n]
	stack := strings.Split(fmt.Sprintln(string(buf[:n])), "\n")
	return stack
}
