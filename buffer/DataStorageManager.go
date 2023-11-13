package buffer

import (
	"os"
)

const MAXPAGES = 50000

type DSMgr struct {
	currFile *os.File
	numPages int
	pages    [MAXPAGES]int
}

func newDSMgr() *DSMgr {
	return &DSMgr{}
}

// 打开文件
func (d *DSMgr) openFile(fileName string) int {
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return 0
	}
	d.currFile = f
	return 1
}

// 关闭文件
func (d *DSMgr) closeFile() int {
	err := d.currFile.Close()
	if err != nil {
		return 0
	} else {
		return 1
	}
}

// 读取某一页
func (d *DSMgr) readPage(pageID int) BFrame {
	d.seek(int64((pageID-1)*FRAMESIZE), 0)
	b := BFrame{}
	d.currFile.Read(b.Filed[:])
	return b
}

// 写入某一页
func (d *DSMgr) writePage(pageID int, frm BFrame) int {
	d.seek(int64((pageID-1)*FRAMESIZE), 0)
	n, _ := d.currFile.Write(frm.Filed[:])
	return n
}

// 偏移文件指针
func (d *DSMgr) seek(offset int64, pos int) int {
	_, err := d.currFile.Seek(offset, pos)
	if err != nil {
		return 0
	} else {
		return 1
	}
}

// 返回当前的文件指针
func (d *DSMgr) GetFile() (file *os.File) {
	return d.currFile
}

func (d *DSMgr) incNumPages() {
	d.numPages += 1
}

func (d *DSMgr) GetNumPages() int {
	return d.numPages
}

func (d *DSMgr) setUse(index, use_bit int) {
	d.pages[index] = use_bit
}

func (d *DSMgr) GetUse(index int) int {
	return d.pages[index]
}
