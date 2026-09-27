package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"
	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrBucketExists        = errors.New("bucket already exists")
	ErrBucketNotEmpty      = errors.New("bucket not empty")
	ErrManifestNotFound    = errors.New("manifest not found")
	ErrMultipartNotFound   = errors.New("multipart upload not found")
	ErrMultipartNotPending = errors.New("multipart upload not pending")
	ErrInvalidPart         = errors.New("invalid or mismatched part")
)

// CreateBucket atomically creates a new bucket record in BadgerDB.
func (s *Store) CreateBucket(ctx context.Context, bucket, ownerID string, tags map[string]string) (*castorv1.BucketRecord, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}

	var created *castorv1.BucketRecord
	key := BucketKey(bucket)

	err := s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == nil {
			var existing castorv1.BucketRecord
			if err := item.Value(func(val []byte) error {
				return proto.Unmarshal(val, &existing)
			}); err != nil {
				return err
			}
			if !existing.IsDeleted {
				return ErrBucketExists
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		created = &castorv1.BucketRecord{
			Name:      bucket,
			OwnerId:   ownerID,
			Tags:      tags,
			IsDeleted: false,
			CreatedAt: timestamppb.Now(),
		}

		val, err := proto.Marshal(created)
		if err != nil {
			return err
		}
		return txn.Set(key, val)
	})
	if err != nil {
		return nil, err
	}
	return created, nil
}

// EnsureBucket creates a bucket if it does not already exist.
func (s *Store) EnsureBucket(ctx context.Context, bucket, ownerID string) error {
	_, err := s.CreateBucket(ctx, bucket, ownerID, nil)
	if errors.Is(err, ErrBucketExists) {
		return nil
	}
	return err
}

// DeleteBucket marks a bucket as deleted if it contains no active manifests.
func (s *Store) DeleteBucket(ctx context.Context, bucket string) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}

	key := BucketKey(bucket)

	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		var record castorv1.BucketRecord
		if err := item.Value(func(val []byte) error {
			return proto.Unmarshal(val, &record)
		}); err != nil {
			return err
		}
		if record.IsDeleted {
			return ErrNotFound
		}

		prefix := ManifestPrefix(bucket)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			var manifest castorv1.ManifestRecord
			if err := it.Item().Value(func(val []byte) error {
				return proto.Unmarshal(val, &manifest)
			}); err != nil {
				return err
			}
			if manifest.Status != "deleted" {
				return ErrBucketNotEmpty
			}
		}

		record.IsDeleted = true
		val, err := proto.Marshal(&record)
		if err != nil {
			return err
		}
		return txn.Set(key, val)
	})
}

// CommitManifest atomically commits an object manifest and updates chunk reference counts.
func (s *Store) CommitManifest(ctx context.Context, req *castorv1.CommitManifestRequest) (*castorv1.ManifestRecord, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}

	manifestKey := ManifestKey(req.Bucket, req.Key)
	bucketKey := BucketKey(req.Bucket)
	now := timestamppb.Now()

	var committedRecord *castorv1.ManifestRecord

	err := s.db.Update(func(txn *badger.Txn) error {
		bItem, err := txn.Get(bucketKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrBucketNotFound
		}
		if err != nil {
			return err
		}
		var bucketRecord castorv1.BucketRecord
		if err := bItem.Value(func(val []byte) error {
			return proto.Unmarshal(val, &bucketRecord)
		}); err != nil {
			return err
		}
		if bucketRecord.IsDeleted {
			return ErrBucketNotFound
		}

		var existingManifest *castorv1.ManifestRecord
		mItem, err := txn.Get(manifestKey)
		if err == nil {
			var m castorv1.ManifestRecord
			if err := mItem.Value(func(val []byte) error {
				return proto.Unmarshal(val, &m)
			}); err != nil {
				return err
			}
			if m.Status != "deleted" {
				existingManifest = &m
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		// Displaced chunks decrement refcounts; zero-ref chunks enter 24h quarantine.
		if existingManifest != nil {
			for _, oldChunkID := range existingManifest.ChunkIds {
				cKey := ChunkKey(oldChunkID)
				cItem, err := txn.Get(cKey)
				if errors.Is(err, badger.ErrKeyNotFound) {
					continue
				}
				if err != nil {
					return err
				}

				var chunkLoc castorv1.ChunkLocationRecord
				if err := cItem.Value(func(val []byte) error {
					return proto.Unmarshal(val, &chunkLoc)
				}); err != nil {
					return err
				}

				if chunkLoc.RefCount > 0 {
					chunkLoc.RefCount--
				}
				if chunkLoc.RefCount == 0 {
					chunkLoc.OrphanedAt = now
				}

				cVal, err := proto.Marshal(&chunkLoc)
				if err != nil {
					return err
				}
				if err := txn.Set(cKey, cVal); err != nil {
					return err
				}
			}
		}

		placementMap := make(map[string]*castorv1.ChunkPlacement, len(req.ChunkPlacements))
		for _, p := range req.ChunkPlacements {
			if p != nil && p.ChunkHash != "" {
				placementMap[p.ChunkHash] = p
			}
		}

		for _, chunkID := range req.ChunkIds {
			cKey := ChunkKey(chunkID)
			cItem, err := txn.Get(cKey)

			var chunkLoc castorv1.ChunkLocationRecord
			if errors.Is(err, badger.ErrKeyNotFound) {
				chunkLoc = castorv1.ChunkLocationRecord{
					ChunkHash: chunkID,
					RefCount:  1,
				}
				if placement, exists := placementMap[chunkID]; exists {
					chunkLoc.Nodes = placement.NodeAddresses
					chunkLoc.Size = placement.Size
				}
			} else if err != nil {
				return err
			} else {
				if err := cItem.Value(func(val []byte) error {
					return proto.Unmarshal(val, &chunkLoc)
				}); err != nil {
					return err
				}
				chunkLoc.RefCount++
				chunkLoc.OrphanedAt = nil

				if placement, exists := placementMap[chunkID]; exists {
					chunkLoc.Nodes = unionNodes(chunkLoc.Nodes, placement.NodeAddresses)
					if chunkLoc.Size == 0 {
						chunkLoc.Size = placement.Size
					}
				}
			}

			cVal, err := proto.Marshal(&chunkLoc)
			if err != nil {
				return err
			}
			if err := txn.Set(cKey, cVal); err != nil {
				return err
			}
		}

		ownerID := bucketRecord.OwnerId
		if req.OwnerId != "" {
			ownerID = req.OwnerId
		} else if existingManifest != nil && existingManifest.OwnerId != "" {
			ownerID = existingManifest.OwnerId
		}

		createdAt := now
		if existingManifest != nil && existingManifest.CreatedAt != nil {
			createdAt = existingManifest.CreatedAt
		}

		committedRecord = &castorv1.ManifestRecord{
			Bucket:      req.Bucket,
			Key:         req.Key,
			Size:        req.Size,
			Etag:        req.Etag,
			ContentType: req.ContentType,
			ChunkIds:    req.ChunkIds,
			Status:      "committed",
			CreatedAt:   createdAt,
			UpdatedAt:   now,
			OwnerId:     ownerID,
		}

		mVal, err := proto.Marshal(committedRecord)
		if err != nil {
			return err
		}
		return txn.Set(manifestKey, mVal)
	})
	if err != nil {
		return nil, err
	}
	return committedRecord, nil
}

// DeleteManifest marks an object manifest as deleted and decrements its chunk reference counts.
func (s *Store) DeleteManifest(ctx context.Context, bucket, key string) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}

	manifestKey := ManifestKey(bucket, key)
	now := timestamppb.Now()

	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(manifestKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		var record castorv1.ManifestRecord
		if err := item.Value(func(val []byte) error {
			return proto.Unmarshal(val, &record)
		}); err != nil {
			return err
		}
		if record.Status == "deleted" {
			return ErrNotFound
		}

		record.Status = "deleted"
		record.UpdatedAt = now

		mVal, err := proto.Marshal(&record)
		if err != nil {
			return err
		}
		if err := txn.Set(manifestKey, mVal); err != nil {
			return err
		}

		// Chunks entering RefCount == 0 are quarantined before GC.
		for _, chunkID := range record.ChunkIds {
			cKey := ChunkKey(chunkID)
			cItem, err := txn.Get(cKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}

			var chunkLoc castorv1.ChunkLocationRecord
			if err := cItem.Value(func(val []byte) error {
				return proto.Unmarshal(val, &chunkLoc)
			}); err != nil {
				return err
			}

			if chunkLoc.RefCount > 0 {
				chunkLoc.RefCount--
			}
			if chunkLoc.RefCount == 0 {
				chunkLoc.OrphanedAt = now
			}

			cVal, err := proto.Marshal(&chunkLoc)
			if err != nil {
				return err
			}
			if err := txn.Set(cKey, cVal); err != nil {
				return err
			}
		}
		return nil
	})
}

// InitiateMultipart starts a new multipart upload session.
func (s *Store) InitiateMultipart(ctx context.Context, uploadID string, req *castorv1.InitiateMultipartMetaRequest) (*castorv1.MultipartRecord, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if uploadID == "" && req.UploadId != "" {
		uploadID = req.UploadId
	}

	bucketKey := BucketKey(req.Bucket)
	mpKey := MultipartKey(uploadID)

	var record *castorv1.MultipartRecord

	err := s.db.Update(func(txn *badger.Txn) error {
		bItem, err := txn.Get(bucketKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrBucketNotFound
		}
		if err != nil {
			return err
		}
		var bRecord castorv1.BucketRecord
		if err := bItem.Value(func(val []byte) error {
			return proto.Unmarshal(val, &bRecord)
		}); err != nil {
			return err
		}
		if bRecord.IsDeleted {
			return ErrBucketNotFound
		}

		record = &castorv1.MultipartRecord{
			UploadId:    uploadID,
			Bucket:      req.Bucket,
			Key:         req.Key,
			ContentType: req.ContentType,
			Status:      "pending",
			CreatedAt:   timestamppb.Now(),
		}

		val, err := proto.Marshal(record)
		if err != nil {
			return err
		}
		return txn.Set(mpKey, val)
	})
	if err != nil {
		return nil, err
	}
	return record, nil
}

// CommitPart records an uploaded part and registers its chunk references.
func (s *Store) CommitPart(ctx context.Context, req *castorv1.CommitPartMetaRequest) (*castorv1.PartRecord, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}

	mpKey := MultipartKey(req.UploadId)
	partKey := MultipartPartKey(req.UploadId, req.PartNumber)

	var part *castorv1.PartRecord

	err := s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(mpKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		var mp castorv1.MultipartRecord
		if err := item.Value(func(val []byte) error {
			return proto.Unmarshal(val, &mp)
		}); err != nil {
			return err
		}
		if mp.Status != "pending" {
			return ErrMultipartNotPending
		}

		part = &castorv1.PartRecord{
			UploadId:   req.UploadId,
			PartNumber: req.PartNumber,
			Size:       req.Size,
			Etag:       req.Etag,
			ChunkIds:   req.ChunkIds,
			UploadedAt: timestamppb.Now(),
		}

		val, err := proto.Marshal(part)
		if err != nil {
			return err
		}
		if err := txn.Set(partKey, val); err != nil {
			return err
		}

		// Protect in-flight chunks from quarantine GC while upload is pending.
		placementMap := make(map[string]*castorv1.ChunkPlacement, len(req.ChunkPlacements))
		for _, p := range req.ChunkPlacements {
			if p != nil && p.ChunkHash != "" {
				placementMap[p.ChunkHash] = p
			}
		}

		for _, chunkID := range req.ChunkIds {
			cKey := ChunkKey(chunkID)
			cItem, err := txn.Get(cKey)

			var chunkLoc castorv1.ChunkLocationRecord
			if errors.Is(err, badger.ErrKeyNotFound) {
				chunkLoc = castorv1.ChunkLocationRecord{
					ChunkHash: chunkID,
					RefCount:  1,
				}
				if placement, exists := placementMap[chunkID]; exists {
					chunkLoc.Nodes = placement.NodeAddresses
					chunkLoc.Size = placement.Size
				}
			} else if err != nil {
				return err
			} else {
				if err := cItem.Value(func(val []byte) error {
					return proto.Unmarshal(val, &chunkLoc)
				}); err != nil {
					return err
				}
				chunkLoc.RefCount++
				chunkLoc.OrphanedAt = nil

				if placement, exists := placementMap[chunkID]; exists {
					chunkLoc.Nodes = unionNodes(chunkLoc.Nodes, placement.NodeAddresses)
					if chunkLoc.Size == 0 {
						chunkLoc.Size = placement.Size
					}
				}
			}

			cVal, err := proto.Marshal(&chunkLoc)
			if err != nil {
				return err
			}
			if err := txn.Set(cKey, cVal); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return part, nil
}

// CompleteMultipart validates all parts, concatenates chunk lists, and commits the object manifest.
func (s *Store) CompleteMultipart(ctx context.Context, req *castorv1.CompleteMultipartMetaRequest) (*castorv1.ManifestRecord, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}

	mpKey := MultipartKey(req.UploadId)
	var finalManifest *castorv1.ManifestRecord

	err := s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(mpKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		var mp castorv1.MultipartRecord
		if err := item.Value(func(val []byte) error {
			return proto.Unmarshal(val, &mp)
		}); err != nil {
			return err
		}
		if mp.Status != "pending" {
			return ErrMultipartNotPending
		}

		// Load stored parts
		prefix := MultipartPartPrefix(req.UploadId)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		var storedParts []*castorv1.PartRecord
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			var p castorv1.PartRecord
			if err := it.Item().Value(func(val []byte) error {
				return proto.Unmarshal(val, &p)
			}); err != nil {
				return err
			}
			storedParts = append(storedParts, &p)
		}

		if len(req.Parts) > 0 {
			if len(storedParts) != len(req.Parts) {
				return ErrInvalidPart
			}
			for i, pReq := range req.Parts {
				sp := storedParts[i]
				if sp.PartNumber != pReq.PartNumber {
					return ErrInvalidPart
				}
				if pReq.Etag != "" && strings.Trim(sp.Etag, "\"") != strings.Trim(pReq.Etag, "\"") {
					return ErrInvalidPart
				}
			}
		}

		var totalSize int64
		var aggregatedChunkIDs []string
		var etagBuilder strings.Builder

		for _, p := range storedParts {
			totalSize += p.Size
			aggregatedChunkIDs = append(aggregatedChunkIDs, p.ChunkIds...)
			etagBuilder.WriteString(p.Etag)
		}

		compositeETag := fmt.Sprintf("%x-%d", sha256.Sum256([]byte(etagBuilder.String())), len(storedParts))

		bItem, err := txn.Get(BucketKey(mp.Bucket))
		if err != nil {
			return err
		}
		var bRecord castorv1.BucketRecord
		if err := bItem.Value(func(val []byte) error {
			return proto.Unmarshal(val, &bRecord)
		}); err != nil {
			return err
		}

		now := timestamppb.Now()
		finalManifest = &castorv1.ManifestRecord{
			Bucket:      mp.Bucket,
			Key:         mp.Key,
			Size:        totalSize,
			Etag:        compositeETag,
			ContentType: mp.ContentType,
			ChunkIds:    aggregatedChunkIDs,
			Status:      "committed",
			CreatedAt:   mp.CreatedAt,
			UpdatedAt:   now,
			OwnerId:     bRecord.OwnerId,
		}

		mVal, err := proto.Marshal(finalManifest)
		if err != nil {
			return err
		}
		if err := txn.Set(ManifestKey(mp.Bucket, mp.Key), mVal); err != nil {
			return err
		}

		mp.Status = "completed"
		mpVal, err := proto.Marshal(&mp)
		if err != nil {
			return err
		}
		if err := txn.Set(mpKey, mpVal); err != nil {
			return err
		}

		for _, p := range storedParts {
			if err := txn.Delete(MultipartPartKey(req.UploadId, p.PartNumber)); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return finalManifest, nil
}

// AbortMultipart cancels a multipart session, deletes staged parts, and decrements chunk refcounts.
func (s *Store) AbortMultipart(ctx context.Context, uploadID string) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}

	mpKey := MultipartKey(uploadID)
	now := timestamppb.Now()

	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(mpKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		var mp castorv1.MultipartRecord
		if err := item.Value(func(val []byte) error {
			return proto.Unmarshal(val, &mp)
		}); err != nil {
			return err
		}
		if mp.Status == "completed" {
			return errors.New("cannot abort completed multipart upload")
		}

		mp.Status = "aborted"
		mpVal, err := proto.Marshal(&mp)
		if err != nil {
			return err
		}
		if err := txn.Set(mpKey, mpVal); err != nil {
			return err
		}

		prefix := MultipartPartPrefix(uploadID)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		var partsToDelete [][]byte
		var chunksToDecrement []string

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			partKey := make([]byte, len(it.Item().Key()))
			copy(partKey, it.Item().Key())
			partsToDelete = append(partsToDelete, partKey)

			var p castorv1.PartRecord
			if err := it.Item().Value(func(val []byte) error {
				return proto.Unmarshal(val, &p)
			}); err != nil {
				return err
			}
			chunksToDecrement = append(chunksToDecrement, p.ChunkIds...)
		}

		for _, chunkID := range chunksToDecrement {
			cKey := ChunkKey(chunkID)
			cItem, err := txn.Get(cKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			var chunkLoc castorv1.ChunkLocationRecord
			if err := cItem.Value(func(val []byte) error {
				return proto.Unmarshal(val, &chunkLoc)
			}); err != nil {
				return err
			}
			if chunkLoc.RefCount > 0 {
				chunkLoc.RefCount--
			}
			if chunkLoc.RefCount == 0 {
				chunkLoc.OrphanedAt = now
			}
			cVal, err := proto.Marshal(&chunkLoc)
			if err != nil {
				return err
			}
			if err := txn.Set(cKey, cVal); err != nil {
				return err
			}
		}

		for _, pKey := range partsToDelete {
			if err := txn.Delete(pKey); err != nil {
				return err
			}
		}

		return nil
	})
}

// UpdateChunkLocation updates known node locations hosting a chunk.
func (s *Store) UpdateChunkLocation(ctx context.Context, chunkHash string, nodeAddresses []string) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}

	key := ChunkKey(chunkHash)

	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)

		var record castorv1.ChunkLocationRecord
		if errors.Is(err, badger.ErrKeyNotFound) {
			record = castorv1.ChunkLocationRecord{
				ChunkHash: chunkHash,
				Nodes:     nodeAddresses,
				RefCount:  0,
			}
		} else if err != nil {
			return err
		} else {
			if err := item.Value(func(val []byte) error {
				return proto.Unmarshal(val, &record)
			}); err != nil {
				return err
			}
			record.Nodes = unionNodes(record.Nodes, nodeAddresses)
		}

		val, err := proto.Marshal(&record)
		if err != nil {
			return err
		}
		return txn.Set(key, val)
	})
}

// RemoveChunkLocations removes node replicas or deletes chunk record if nodes list is empty.
func (s *Store) RemoveChunkLocations(ctx context.Context, chunkHash string, nodeAddresses []string) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}

	key := ChunkKey(chunkHash)

	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
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

		if len(nodeAddresses) == 0 {
			return txn.Delete(key)
		}

		removeSet := make(map[string]bool, len(nodeAddresses))
		for _, addr := range nodeAddresses {
			removeSet[addr] = true
		}

		var remaining []string
		for _, node := range record.Nodes {
			if !removeSet[node] {
				remaining = append(remaining, node)
			}
		}

		if len(remaining) == 0 && record.RefCount == 0 {
			return txn.Delete(key)
		}

		record.Nodes = remaining
		val, err := proto.Marshal(&record)
		if err != nil {
			return err
		}
		return txn.Set(key, val)
	})
}

// DeleteChunkLocation completely deletes a chunk location record.
func (s *Store) DeleteChunkLocation(ctx context.Context, chunkHash string) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(ChunkKey(chunkHash))
	})
}

func unionNodes(existing []string, incoming []string) []string {
	seen := make(map[string]bool, len(existing)+len(incoming))
	var result []string
	for _, node := range existing {
		if !seen[node] {
			seen[node] = true
			result = append(result, node)
		}
	}
	for _, node := range incoming {
		if !seen[node] {
			seen[node] = true
			result = append(result, node)
		}
	}
	return result
}
