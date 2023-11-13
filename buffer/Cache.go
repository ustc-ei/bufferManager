package buffer

import (
	"bytes"
	"fmt"
)

type Cache interface {
	Insert(frameID int)
	GetVictim() (frameID int)
	Query(frameID int)
	RemoveEle(frameID int)
}

type linkNode struct {
	frameID int
	val     int
	pre     *linkNode
	post    *linkNode
}

type lruCache struct {
	head      *linkNode
	tail      *linkNode
	MaxLength int
	curLength int
	hash      map[int]*linkNode
}

func NewLRUCache(maxLength int) *lruCache {
	// LRUCahe 的实例化
	return &lruCache{
		MaxLength: maxLength,
		hash:      make(map[int]*linkNode),
	}
}

func (l *lruCache) IsFull() bool {
	return l.curLength == l.MaxLength
}

func (l *lruCache) IsEmpty() bool {
	return l.curLength == 0
}

/*
向 Cache 中插入
*/
func (l *lruCache) Insert(frameID int) {
	var newNode *linkNode
	if l.IsEmpty() { // cache 为空，则将 head 和 tail 都设置为当前插入的节点
		newNode = &linkNode{frameID: frameID, val: 1, pre: nil, post: nil}
		l.tail = newNode
	} else { // 否则, 将插入到头部
		newNode = &linkNode{frameID: frameID, val: 1, pre: nil, post: l.head}
		l.head.pre = newNode
	}
	l.head = newNode
	// 存储在哈希表中, 保证 O(1) 时间消耗
	l.hash[newNode.frameID] = newNode
	if !l.IsFull() {
		l.curLength += 1
	}
}

func (l *lruCache) RemoveEle(frameID int) {
	node := l.hash[frameID]
	if node == l.head && node == l.tail {
		l.head, l.tail = nil, nil
	} else if node == l.tail {
		l.tail = node.pre
		l.tail.post = nil
	} else if node == l.head {
		l.head = node.post
		l.head.pre = nil
	} else {
		node.pre.post = node.post
		node.post.pre = node.pre
	}
	delete(l.hash, frameID)
	l.curLength -= 1
}

/*
替换页的 frameID
*/
func (l *lruCache) GetVictim() (frameID int) {
	frameID = l.tail.frameID
	return
}

/*
frameID 是否在 Cache 中
*/
func (l *lruCache) isInLRU(frameID int) bool {
	_, ok := l.hash[frameID]
	return ok
}

/*
cache 查询操作
*/
func (l *lruCache) Query(frameID int) {
	if l.isInLRU(frameID) {
		queryNode := l.hash[frameID]
		queryNode.val += 1
		if queryNode == l.tail && queryNode == l.head {
			return
		} else if queryNode == l.head {
			return
		} else if queryNode == l.tail {
			queryNode.pre.post = nil
			l.tail = queryNode.pre
		} else { // 访问的节点是头部和尾部之间的节点
			queryNode.pre.post = queryNode.post
			queryNode.post.pre = queryNode.pre
		}
		// 修改 cache 中的 head
		l.head.pre = queryNode
		queryNode.post = l.head
		queryNode.pre = nil
		l.head = queryNode
	}
}

type lru2Cache struct {
	k           int
	historyList *lruCache
	bufferList  *lruCache
}

func NewLRU2Cache(k int, maxLength int) *lru2Cache {
	return &lru2Cache{
		k:           k,
		historyList: NewLRUCache(maxLength),
		bufferList:  NewLRUCache(maxLength),
	}
}

func (l *lru2Cache) Insert(frameID int) {
	l.historyList.Insert(frameID)
}

func (l *lru2Cache) Query(frameID int) {
	if l.historyList.isInLRU(frameID) {
		l.historyList.Query(frameID)
		historyListHead := l.historyList.head
		if historyListHead.val >= l.k {
			l.bufferList.Insert(historyListHead.frameID)
			l.historyList.RemoveEle(historyListHead.frameID)
		}
	} else {
		l.bufferList.Query(frameID)
	}
}

func (l *lru2Cache) GetVictim() (frameID int) {
	if l.historyList.IsEmpty() {
		frameID = l.bufferList.GetVictim()
	} else {
		frameID = l.historyList.GetVictim()
	}
	return
}

func (l *lru2Cache) RemoveEle(frameID int) {
	if l.historyList.isInLRU(frameID) {
		l.historyList.RemoveEle(frameID)
	} else {
		l.bufferList.RemoveEle(frameID)
	}
}

func (l *lru2Cache) String() string {
	var buf bytes.Buffer
	buf.Write([]byte("{\n"))
	buf.Write([]byte("\thistoryList: "))
	for p := l.historyList.head; p != nil; p = p.post {
		if p.post != nil {
			fmt.Fprintf(&buf, "%d ", p.frameID)
		} else {
			fmt.Fprintf(&buf, "%d", p.frameID)
		}
	}
	buf.Write([]byte("\n\tbufferList: "))
	for p := l.bufferList.head; p != nil; p = p.post {
		if p.post != nil {
			fmt.Fprintf(&buf, "%d ", p.frameID)
		} else {
			fmt.Fprintf(&buf, "%d", p.frameID)
		}
	}
	buf.Write([]byte("\n}"))
	return buf.String()
}
