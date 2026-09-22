package storage

import (
	"os"
	"syscall"
)

type DiskStats struct {
	TotalBytes uint64
	UsedBytes  uint64
	FreeBytes  uint64
}

func (s *Storage) GetDiskStats() (DiskStats, error) {
	var stat syscall.Statfs_t

	path, _ := os.UserHomeDir()

	if err := syscall.Statfs(path, &stat); err != nil {
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
