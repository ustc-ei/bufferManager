package buffer

import (
	"fmt"
	"math/rand"
)

type BMgr struct {
	freeFramesNum int                // the num of free frame
	ftop          [DEFBUFSIZE]int    // frameID to pageID map
	ptof          [DEFBUFSIZE]*BCB   // use pageID % DEFBUFSIZE to calculate the index, if two pageID get the same index, use the linklist
	cache         Cache              // cache
	dsmgr         DSMgr              // data storage manager
	Buf           [DEFBUFSIZE]BFrame // buffer
	hitCount      int
	readDiskIO    uint64
	writeDiskIO   uint64
}

func NewBMgr(isLRU bool, k int) *BMgr {
	if isLRU {
		return &BMgr{
			freeFramesNum: DEFBUFSIZE,
			cache:         NewLRUCache(DEFBUFSIZE),
			dsmgr:         *newDSMgr(),
		}
	} else {
		return &BMgr{
			freeFramesNum: DEFBUFSIZE,
			cache:         NewLRU2Cache(k, DEFBUFSIZE),
			dsmgr:         *newDSMgr(),
		}
	}
}

/*
获取 pageID 对应的 BCB 块
*/
func (b *BMgr) getBCB(pageID int) *BCB {
	frameIndex := b.hash(pageID)
	for p := b.ptof[frameIndex]; p != nil; p = p.next {
		if p.pageID == pageID {
			return p
		}
	}
	return nil
}

/*
进行读取
*/
func (b *BMgr) FixPage(pageID, prot int) (frameID int) {
	bcb := b.getBCB(pageID)
	if bcb != nil { // 如果请求的页在缓冲区中
		b.hitCount += 1
		frameID = bcb.frameID
	} else { // 不在缓冲区中, 将页从磁盘中取出并加入到 frame 中
		if b.freeFramesNum != 0 { // 缓冲区没满
			frameID = DEFBUFSIZE - b.freeFramesNum
			b.freeFramesNum--
		} else { // 缓冲区满了
			frameID = b.selectVictim()
			b.removeLRUEle(frameID)
			b.removeBCB(b.ftop[frameID])
		}
		b.addBCB(pageID, frameID)
		b.ftop[frameID] = pageID
		b.Buf[frameID] = b.dsmgr.readPage(pageID)
		b.incReadIO()
		b.cache.Insert(frameID)
	}
	switch prot {
	case 0: // 表示读数据
		b.cache.Query(frameID)
	default: // 表示写
		// TODO: 修改 buf 中对应 frameID 的数据
		b.cache.Query(frameID)
		b.setDirty(frameID)
	}
	return
}

func (b *BMgr) HitCount() int {
	return b.hitCount
}

func (b *BMgr) Init(fileName string) {
	b.dsmgr.openFile(fileName)
}

/*
添加新的页
*/
func (b *BMgr) FixNewPage() int {
	b.dsmgr.incNumPages()
	pageID := b.dsmgr.GetNumPages()
	bytes := make([]byte, FRAMESIZE)
	for i := 0; i < FRAMESIZE; i++ {
		bytes[i] = byte(rand.Intn(256))
	}
	b.dsmgr.writePage(pageID, BFrame{Filed: [4096]byte(bytes)})
	b.dsmgr.setUse(pageID-1, 1)
	return pageID
}

// func (b *BMgr) UnfixPage(pageID int) (frameID int) {
// 	return
// }

/*
目前有多少个空 frame
*/
func (b *BMgr) NumFreeFrames() int {
	return b.freeFramesNum
}

/*
选取替换页的下标
*/
func (b *BMgr) selectVictim() (frameID int) {
	frameID = b.cache.GetVictim()
	return
}

func (b *BMgr) PrintFrame(frameID int) {
	fmt.Println(b.Buf[frameID].Filed)
}

/*
pageID 到 ptof 下标的哈希映射函数
*/
func (b *BMgr) hash(pageID int) (frameIndex int) {
	frameIndex = pageID % DEFBUFSIZE
	return
}

func (b *BMgr) incReadIO() {
	b.readDiskIO += FRAMESIZE
}

func (b *BMgr) incWriteIO() {
	b.writeDiskIO += FRAMESIZE
}

/*
添加 BCB 块
*/
func (b *BMgr) addBCB(pageID, frameID int) {
	frameIndex := b.hash(pageID)
	head := b.ptof[frameIndex]
	if head == nil {
		b.ptof[frameIndex] = &BCB{
			pageID:  pageID,
			frameID: frameID,
		}
		return
	}
	for p := head; ; p = p.next {
		if p.next == nil {
			p.next = &BCB{
				pageID:  pageID,
				frameID: frameID,
			}
			return
		}
	}
}

func (b *BMgr) ReadDiskIO() uint64 {
	return b.readDiskIO
}

func (b *BMgr) WriteDiskIO() uint64 {
	return b.writeDiskIO
}

func (b *BMgr) removeBCB(pageID int) {
	var pre *BCB
	var p *BCB
	frameIndex := b.hash(pageID)
	head := b.ptof[frameIndex]
	for p = head; p != nil; p = p.next { // 遍历链表, 找到对应的 BCB 节点, 将其删除
		if p.pageID == pageID {
			break
		}
		pre = p
	}
	if pre != nil {
		pre.next = p.next
	} else {
		b.ptof[frameIndex] = p.next
	}
	if p.dirty == 1 {
		b.dsmgr.writePage(p.pageID, b.Buf[p.frameID])
		b.incWriteIO()
	}
}

func (b *BMgr) removeLRUEle(frameID int) {
	b.cache.RemoveEle(frameID)
}

func (b *BMgr) setDirty(frameID int) {
	pageID := b.ftop[frameID]
	bcb := b.getBCB(pageID)
	bcb.dirty = 1
}

// func (b *BMgr) unsetDirty(frameID int) {
// 	pageID := b.ftop[frameID]
// 	bcb := b.getBCB(pageID)
// 	bcb.dirty = 0
// }

func (b *BMgr) End() {
	b.writeDirtys()
	b.dsmgr.closeFile()
}

func (b *BMgr) writeDirtys() {
	for _, bcb := range b.ptof {
		for p := bcb; p != nil; p = p.next {
			if p.dirty != 0 {
				b.dsmgr.writePage(p.frameID, b.Buf[p.frameID])
				b.incWriteIO()
			}
		}
	}
}
