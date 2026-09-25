package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"google.golang.org/protobuf/proto"
)

var (
	ErrNotFound       = errors.New("record not found")
	ErrBucketNotFound = errors.New("bucket not found")
)

type Store struct {
	db *badger.DB
}

func Open(dir string, inMem bool) (*Store, error) {
	var opts badger.Options

	if inMem {
		opts = badger.DefaultOptions("").
			WithInMemory(true).
			WithLogger(nil)
	} else {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create badger directory %q: %w", dir, err)
		}
		opts = badger.DefaultOptions(dir).
			WithLogger(nil).
			WithSyncWrites(false).
			WithNumVersionsToKeep(1)
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badgerdb: %w", err)
	}

	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) DB() *badger.DB {
	return s.db
}

func (s *Store) GetBucket(ctx context.Context, name string) (*castorv1.BucketRecord, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}

	var record castorv1.BucketRecord
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(BucketKey(name))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return proto.Unmarshal(val, &record)
		})
	})
	if err != nil {
		return nil, err
	}

	if record.IsDeleted {
		return nil, ErrNotFound
	}
	return &record, nil
}

func (s *Store) CheckBucketExists(ctx context.Context, name string) (bool, error) {
	_, err := s.GetBucket(ctx, name)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) ListBuckets(ctx context.Context) ([]*castorv1.BucketRecord, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}

	var buckets []*castorv1.BucketRecord
	prefix := BucketPrefix()

	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if ctx != nil && ctx.Err() != nil {
				return ctx.Err()
			}
			item := it.Item()
			var record castorv1.BucketRecord
			if err := item.Value(func(val []byte) error {
				return proto.Unmarshal(val, &record)
			}); err != nil {
				return err
			}
			if !record.IsDeleted {
				buckets = append(buckets, &record)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return buckets, nil
}

func (s *Store) GetManifest(ctx context.Context, bucket, key string) (*castorv1.ManifestRecord, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}

	var record castorv1.ManifestRecord
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(ManifestKey(bucket, key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return proto.Unmarshal(val, &record)
		})
	})
	if err != nil {
		return nil, err
	}

	if record.Status == "deleted" {
		return nil, ErrNotFound
	}
	return &record, nil
}

type ListManifestsResult struct {
	Manifests      []*castorv1.ManifestRecord
	CommonPrefixes []string
	NextMarker     string
	IsTruncated    bool
}

func (s *Store) ListManifests(ctx context.Context, bucket, prefix, delimiter, marker string, maxKeys int32) (*ListManifestsResult, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}

	result := &ListManifestsResult{
		Manifests:      make([]*castorv1.ManifestRecord, 0),
		CommonPrefixes: make([]string, 0),
	}

	searchPrefix := ManifestKey(bucket, prefix)
	bucketPrefix := ManifestPrefix(bucket)
	commonPrefixesSeen := make(map[string]bool)

	limit := int(maxKeys)
	if limit <= 0 {
		limit = 1000
	}

	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		startKey := searchPrefix
		if marker != "" {
			startKey = ManifestKey(bucket, marker)
		}

		it.Seek(startKey)

		// Skip marker itself if encountered exactly
		if marker != "" && it.Valid() && bytes.Equal(it.Item().Key(), ManifestKey(bucket, marker)) {
			it.Next()
		}

		for ; it.ValidForPrefix(searchPrefix); it.Next() {
			if ctx != nil && ctx.Err() != nil {
				return ctx.Err()
			}

			currentCount := len(result.Manifests) + len(result.CommonPrefixes)
			if currentCount >= limit {
				result.IsTruncated = true
				if len(result.Manifests) > 0 {
					result.NextMarker = result.Manifests[len(result.Manifests)-1].Key
				}
				break
			}

			item := it.Item()
			fullKey := string(item.Key())
			objectKey := strings.TrimPrefix(fullKey, string(bucketPrefix))

			var record castorv1.ManifestRecord
			if err := item.Value(func(val []byte) error {
				return proto.Unmarshal(val, &record)
			}); err != nil {
				return err
			}

			if record.Status == "deleted" {
				continue
			}

			if delimiter != "" {
				afterPrefix := strings.TrimPrefix(objectKey, prefix)
				idx := strings.Index(afterPrefix, delimiter)
				if idx >= 0 {
					commonPrefix := prefix + afterPrefix[:idx+len(delimiter)]
					if !commonPrefixesSeen[commonPrefix] {
						commonPrefixesSeen[commonPrefix] = true
						result.CommonPrefixes = append(result.CommonPrefixes, commonPrefix)
					}
					continue
				}
			}

			result.Manifests = append(result.Manifests, &record)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (s *Store) GetChunkLocation(ctx context.Context, sha256 string) (*castorv1.ChunkLocationRecord, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}

	var record castorv1.ChunkLocationRecord
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(ChunkKey(sha256))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return proto.Unmarshal(val, &record)
		})
	})
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func (s *Store) CheckChunks(ctx context.Context, hashes []string) (map[string]bool, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}

	result := make(map[string]bool, len(hashes))
	err := s.db.View(func(txn *badger.Txn) error {
		for _, h := range hashes {
			item, err := txn.Get(ChunkKey(h))
			if errors.Is(err, badger.ErrKeyNotFound) {
				result[h] = false
				continue
			}
			if err != nil {
				return err
			}

			var record castorv1.ChunkLocationRecord
			if err := item.Value(func(val []byte) error {
				return proto.Unmarshal(val, &record)
			}); err != nil {
				return err
			}
			result[h] = record.RefCount > 0
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Store) GetMultipart(ctx context.Context, uploadID string) (*castorv1.MultipartRecord, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}

	var record castorv1.MultipartRecord
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(MultipartKey(uploadID))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return proto.Unmarshal(val, &record)
		})
	})
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func (s *Store) GetPart(ctx context.Context, uploadID string, partNumber int32) (*castorv1.PartRecord, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}

	var record castorv1.PartRecord
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(MultipartPartKey(uploadID, partNumber))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return proto.Unmarshal(val, &record)
		})
	})
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func (s *Store) ListParts(ctx context.Context, uploadID string) ([]*castorv1.PartRecord, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}

	var parts []*castorv1.PartRecord
	prefix := MultipartPartPrefix(uploadID)

	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if ctx != nil && ctx.Err() != nil {
				return ctx.Err()
			}
			item := it.Item()
			var part castorv1.PartRecord
			if err := item.Value(func(val []byte) error {
				return proto.Unmarshal(val, &part)
			}); err != nil {
				return err
			}
			parts = append(parts, &part)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return parts, nil
}

func (s *Store) ScanOrphanedChunks(ctx context.Context, quarantineTTL time.Duration, limit int) ([]*castorv1.ChunkLocationRecord, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}

	var orphans []*castorv1.ChunkLocationRecord
	prefix := ChunkPrefix()
	now := time.Now()

	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if ctx != nil && ctx.Err() != nil {
				return ctx.Err()
			}
			if limit > 0 && len(orphans) >= limit {
				break
			}

			item := it.Item()
			var chunk castorv1.ChunkLocationRecord
			if err := item.Value(func(val []byte) error {
				return proto.Unmarshal(val, &chunk)
			}); err != nil {
				return err
			}

			if chunk.RefCount == 0 && chunk.OrphanedAt != nil {
				orphanedTime := chunk.OrphanedAt.AsTime()
				if now.Sub(orphanedTime) >= quarantineTTL {
					orphans = append(orphans, &chunk)
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return orphans, nil
}

func (s *Store) ScanUnderReplicatedChunks(ctx context.Context, targetReplicas int, limit int) ([]*castorv1.ChunkLocationRecord, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}

	var underReplicated []*castorv1.ChunkLocationRecord
	prefix := ChunkPrefix()

	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if ctx != nil && ctx.Err() != nil {
				return ctx.Err()
			}
			if limit > 0 && len(underReplicated) >= limit {
				break
			}

			item := it.Item()
			var chunk castorv1.ChunkLocationRecord
			if err := item.Value(func(val []byte) error {
				return proto.Unmarshal(val, &chunk)
			}); err != nil {
				return err
			}

			if chunk.RefCount > 0 && len(chunk.Nodes) < targetReplicas {
				underReplicated = append(underReplicated, &chunk)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return underReplicated, nil
}
