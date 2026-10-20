package storage

import (
	"syscall"

	"github.com/tharunn0/castor/internal/data/telemetry"
)

type DiskStats struct {
	TotalBytes uint64
	UsedBytes  uint64
	FreeBytes  uint64
}

func (s *Storage) RootDir() string {
	return s.rootDir
}

func (s *Storage) GetDiskStats() (DiskStats, error) {
	var stat syscall.Statfs_t

	if err := syscall.Statfs(s.rootDir, &stat); err != nil {
		return DiskStats{}, err
	}

	bsize := uint64(stat.Bsize)

	// Total capacity
	total := stat.Blocks * bsize

	free := stat.Bavail * bsize

	used := (stat.Blocks - stat.Bfree) * bsize

	return DiskStats{
		TotalBytes: total,
		FreeBytes:  free,
		UsedBytes:  used,
	}, nil
}

func (s *Storage) UpdateCapacityMetrics(metrics *telemetry.DataMetrics) error {
	m := metrics
	if m == nil {
		m = s.metrics
	}
	if m == nil || m.DiskCapacity == nil {
		return nil
	}
	stats, err := s.GetDiskStats()
	if err != nil {
		return err
	}

	m.DiskCapacity.WithLabelValues("total").Set(float64(stats.TotalBytes))
	m.DiskCapacity.WithLabelValues("used").Set(float64(stats.UsedBytes))
	m.DiskCapacity.WithLabelValues("free").Set(float64(stats.FreeBytes))
	return nil
}
