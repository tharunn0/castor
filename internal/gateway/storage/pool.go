package storage

import (
	"sync"
)

type BufferPool struct {
	pool      sync.Pool
	chunkSize int64
	sem       chan struct{}
}

func NewBufferPool(chunkSize int64, maxConcurrent int) *BufferPool {
	return &BufferPool{
		chunkSize: chunkSize,
		sem:       make(chan struct{}, maxConcurrent),
		pool: sync.Pool{
			New: func() any {
				buf := make([]byte, chunkSize)
				return &buf
			},
		},
	}
}

func (p *BufferPool) Acquire() *[]byte {
	p.sem <- struct{}{}
	return p.pool.Get().(*[]byte)
}

func (p *BufferPool) Release(buf *[]byte) {
	p.pool.Put(buf)
	<-p.sem
}
