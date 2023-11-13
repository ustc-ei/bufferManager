package buffer

const FRAMESIZE = 4096
const DEFBUFSIZE = 1024

type BFrame struct {
	Filed [FRAMESIZE]byte
}
