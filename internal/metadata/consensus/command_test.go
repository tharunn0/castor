package consensus

import (
	"testing"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
)

func TestCommandRoundTrip_CreateBucket(t *testing.T) {
	req := &castorv1.CreateBucketMetadataRequest{
		Bucket: "my-bucket",
		Tags:   map[string]string{"env": "prod"},
	}

	cmd, err := NewCreateBucketCommand(req)
	if err != nil {
		t.Fatalf("failed to create command: %v", err)
	}

	encoded, err := cmd.Encode()
	if err != nil {
		t.Fatalf("failed to encode command: %v", err)
	}

	decodedCmd, err := DecodeCommand(encoded)
	if err != nil {
		t.Fatalf("failed to decode command: %v", err)
	}

	if decodedCmd.Type != CmdCreateBucket {
		t.Fatalf("expected CmdCreateBucket, got %s", decodedCmd.Type)
	}

	decodedReq, err := decodedCmd.DecodeCreateBucket()
	if err != nil {
		t.Fatalf("failed to decode create bucket req: %v", err)
	}

	if decodedReq.Bucket != req.Bucket {
		t.Fatalf("expected bucket %s, got %s", req.Bucket, decodedReq.Bucket)
	}
	if decodedReq.Tags["env"] != "prod" {
		t.Fatalf("expected tag env=prod, got %v", decodedReq.Tags)
	}
}

func TestCommandRoundTrip_CommitManifest(t *testing.T) {
	req := &castorv1.CommitManifestRequest{
		Bucket:      "my-bucket",
		Key:         "photos/pic.jpg",
		Size:        1024,
		Etag:        "abcd1234efgh5678",
		ContentType: "image/jpeg",
		ChunkIds:    []string{"chunk-1", "chunk-2"},
		ChunkPlacements: []*castorv1.ChunkPlacement{
			{
				ChunkHash:     "chunk-1",
				NodeAddresses: []string{"10.0.0.1:9101", "10.0.0.2:9102"},
				Size:          512,
			},
			{
				ChunkHash:     "chunk-2",
				NodeAddresses: []string{"10.0.0.2:9102", "10.0.0.3:9103"},
				Size:          512,
			},
		},
		UserMetadata: map[string]string{"uploader": "alice"},
	}

	cmd, err := NewCommitManifestCommand(req)
	if err != nil {
		t.Fatalf("failed to create command: %v", err)
	}

	encoded, err := cmd.Encode()
	if err != nil {
		t.Fatalf("failed to encode command: %v", err)
	}

	decodedCmd, err := DecodeCommand(encoded)
	if err != nil {
		t.Fatalf("failed to decode command: %v", err)
	}

	if decodedCmd.Type != CmdCommitManifest {
		t.Fatalf("expected CmdCommitManifest, got %s", decodedCmd.Type)
	}

	decodedReq, err := decodedCmd.DecodeCommitManifest()
	if err != nil {
		t.Fatalf("failed to decode commit manifest req: %v", err)
	}

	if decodedReq.Bucket != req.Bucket || decodedReq.Key != req.Key {
		t.Fatalf("manifest bucket/key mismatch: %v vs %v", decodedReq, req)
	}
	if len(decodedReq.ChunkPlacements) != 2 {
		t.Fatalf("expected 2 chunk placements, got %d", len(decodedReq.ChunkPlacements))
	}
}

func TestCommandErrors(t *testing.T) {
	t.Run("empty bytes", func(t *testing.T) {
		_, err := DecodeCommand(nil)
		if err == nil {
			t.Fatal("expected error on nil bytes")
		}
	})

	t.Run("unknown command type", func(t *testing.T) {
		_, err := DecodeCommand([]byte{255, 1, 2, 3})
		if err == nil {
			t.Fatal("expected error on unknown command type")
		}
	})

	t.Run("type mismatch", func(t *testing.T) {
		req := &castorv1.CreateBucketMetadataRequest{Bucket: "test"}
		cmd, _ := NewCreateBucketCommand(req)
		_, err := cmd.DecodeCommitManifest()
		if err == nil {
			t.Fatal("expected error on type mismatch decoding")
		}
	})
}
