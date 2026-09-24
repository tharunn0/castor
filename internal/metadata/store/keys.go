package store

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	ErrInvalidKey = errors.New("invalid storage key format")
)

const (
	PrefixBucket        = "bucket:"
	PrefixManifest      = "manifest:"
	PrefixChunk         = "chunk:"
	PrefixMultipart     = "multipart:"
	PrefixMultipartPart = "multipart_part:"
)

// Bucket Keys: bucket:<bucket>
func BucketKey(bucket string) []byte {
	return []byte(PrefixBucket + bucket)
}

func BucketPrefix() []byte {
	return []byte(PrefixBucket)
}

func ParseBucketKey(key []byte) (string, error) {
	s := string(key)
	if !strings.HasPrefix(s, PrefixBucket) {
		return "", fmt.Errorf("%w: missing bucket prefix in %q", ErrInvalidKey, s)
	}
	bucket := strings.TrimPrefix(s, PrefixBucket)
	if bucket == "" {
		return "", fmt.Errorf("%w: empty bucket name in %q", ErrInvalidKey, s)
	}
	return bucket, nil
}

// Manifest Keys: manifest:<bucket>:<key>
func ManifestKey(bucket, objectKey string) []byte {
	return []byte(PrefixManifest + bucket + ":" + objectKey)
}

func ManifestPrefix(bucket string) []byte {
	return []byte(PrefixManifest + bucket + ":")
}

func AllManifestsPrefix() []byte {
	return []byte(PrefixManifest)
}

func ParseManifestKey(key []byte) (bucket string, objectKey string, err error) {
	s := string(key)
	if !strings.HasPrefix(s, PrefixManifest) {
		return "", "", fmt.Errorf("%w: missing manifest prefix in %q", ErrInvalidKey, s)
	}
	rest := strings.TrimPrefix(s, PrefixManifest)
	parts := strings.SplitN(rest, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("%w: invalid manifest key format %q", ErrInvalidKey, s)
	}
	return parts[0], parts[1], nil
}

// Chunk Keys: chunk:<sha256>
func ChunkKey(chunkHash string) []byte {
	return []byte(PrefixChunk + chunkHash)
}

func ChunkPrefix() []byte {
	return []byte(PrefixChunk)
}

func ParseChunkKey(key []byte) (string, error) {
	s := string(key)
	if !strings.HasPrefix(s, PrefixChunk) {
		return "", fmt.Errorf("%w: missing chunk prefix in %q", ErrInvalidKey, s)
	}
	hash := strings.TrimPrefix(s, PrefixChunk)
	if hash == "" {
		return "", fmt.Errorf("%w: empty chunk hash in %q", ErrInvalidKey, s)
	}
	return hash, nil
}

// Multipart Keys: multipart:<upload_id>
func MultipartKey(uploadID string) []byte {
	return []byte(PrefixMultipart + uploadID)
}

func MultipartPrefix() []byte {
	return []byte(PrefixMultipart)
}

func ParseMultipartKey(key []byte) (string, error) {
	s := string(key)
	if !strings.HasPrefix(s, PrefixMultipart) {
		return "", fmt.Errorf("%w: missing multipart prefix in %q", ErrInvalidKey, s)
	}
	uploadID := strings.TrimPrefix(s, PrefixMultipart)
	if uploadID == "" {
		return "", fmt.Errorf("%w: empty upload id in %q", ErrInvalidKey, s)
	}
	return uploadID, nil
}

// Multipart Part Keys: multipart_part:<upload_id>:<part_num> (5-digit zero-padded)
func MultipartPartKey(uploadID string, partNum int32) []byte {
	return fmt.Appendf(nil, "%s%s:%05d", PrefixMultipartPart, uploadID, partNum)
}

func MultipartPartPrefix(uploadID string) []byte {
	return fmt.Appendf(nil, "%s%s:", PrefixMultipartPart, uploadID)
}

func ParseMultipartPartKey(key []byte) (uploadID string, partNum int32, err error) {
	s := string(key)
	if !strings.HasPrefix(s, PrefixMultipartPart) {
		return "", 0, fmt.Errorf("%w: missing multipart_part prefix in %q", ErrInvalidKey, s)
	}
	rest := strings.TrimPrefix(s, PrefixMultipartPart)
	parts := strings.SplitN(rest, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", 0, fmt.Errorf("%w: invalid multipart part key format %q", ErrInvalidKey, s)
	}

	num, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return "", 0, fmt.Errorf("%w: invalid part number %q in %q", ErrInvalidKey, parts[1], s)
	}
	return parts[0], int32(num), nil
}
