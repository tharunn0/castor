package storage

import (
	"sync"

	gwtelemetry "github.com/tharunn0/castor/internal/gateway/telemetry"
)

type BufferPool struct {
	pool      sync.Pool
	chunkSize int64
	sem       chan struct{}
	metrics   *gwtelemetry.GatewayMetrics
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

func (p *BufferPool) WithMetrics(m *gwtelemetry.GatewayMetrics) *BufferPool {
	p.metrics = m
	return p
}

func (p *BufferPool) Acquire() *[]byte {
	p.sem <- struct{}{}
	if p.metrics != nil {
		if p.metrics.ActiveUploads != nil {
			p.metrics.ActiveUploads.Inc()
		}
		if p.metrics.BufferPoolInUse != nil {
			p.metrics.BufferPoolInUse.Add(float64(p.chunkSize))
		}
	}
	return p.pool.Get().(*[]byte)
}

func (p *BufferPool) Release(buf *[]byte) {
	p.pool.Put(buf)
	if p.metrics != nil {
		if p.metrics.BufferPoolInUse != nil {
			p.metrics.BufferPoolInUse.Sub(float64(p.chunkSize))
		}
		if p.metrics.ActiveUploads != nil {
			p.metrics.ActiveUploads.Dec()
		}
	}
	<-p.sem
}
