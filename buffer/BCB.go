package buffer

type BCB struct {
	pageID  int
	frameID int
	// latch   int
	// count   int
	dirty int
	next  *BCB
}
