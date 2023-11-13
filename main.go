package main

import (
	"buffer/buffer"
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

func splitModePageID(s string) (mode, pageID int) {
	splitNums := strings.Split(s, ",")
	mode, _ = strconv.Atoi(splitNums[0])
	pageID, _ = strconv.Atoi(splitNums[1])
	return
}

func calSpeed(isLRU bool, k int) {
	bufferManager := buffer.NewBMgr(isLRU, k)
	bufferManager.Init("./data.dbf")
	defer bufferManager.End()
	for i := 1; i <= 50000; i++ {
		bufferManager.FixNewPage()
	}
	f, _ := os.OpenFile("./data-5w-50w-zipf.txt", os.O_RDONLY, 0644)
	reader := bufio.NewReader(f)
	start := time.Now()
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		mode, pageID := splitModePageID(string(line))
		bufferManager.FixPage(pageID, mode)
	}
	end := time.Since(start)
	fmt.Println()
	fmt.Printf("Hit Count is %d\n", bufferManager.HitCount())
	fmt.Printf("runtime is %fs\n", end.Seconds())
	fmt.Printf("ReadBytes: %d bytes\n", bufferManager.ReadDiskIO())
	fmt.Printf("WriteBytes: %d bytes", bufferManager.WriteDiskIO())
	fmt.Println()
}

func main() {
	fmt.Println("lru: ")
	calSpeed(true, 2)
	for k := 2; k <= 5; k++ {
		printString := fmt.Sprintf("lru%d: ", k)
		fmt.Println(printString)
		calSpeed(false, k)
	}
}
