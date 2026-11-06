package backend

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/gateway/config"
	"github.com/tharunn0/castor/internal/gateway/storage"
	"github.com/versity/versitygw/s3err"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMapUnavailable(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantSlow bool
	}{
		{name: "nil", err: nil, wantSlow: false},
		{name: "metadata unavailable", err: fmt.Errorf("%w: no leader", storage.ErrMetadataUnavailable), wantSlow: true},
		{name: "quorum not met", err: fmt.Errorf("%w: boom", storage.ErrQuorumNotMet), wantSlow: true},
		{name: "no nodes", err: storage.ErrNoNodes, wantSlow: true},
		{name: "bucket not found passes through", err: storage.ErrBucketNotFound, wantSlow: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapUnavailable(tt.err)
			var apiErr s3err.APIError
			isSlow := errors.As(got, &apiErr) && apiErr.Code == "SlowDown" && apiErr.HTTPStatusCode == http.StatusServiceUnavailable
			if isSlow != tt.wantSlow {
				t.Fatalf("mapUnavailable(%v) = %v, want SlowDown=%v", tt.err, got, tt.wantSlow)
			}
			if !tt.wantSlow && !errors.Is(got, tt.err) && got != tt.err {
				t.Fatalf("mapUnavailable(%v) changed a non-availability error to %v", tt.err, got)
			}
		})
	}
}

type leaderlessMetadataServer struct {
	castorv1.UnimplementedMetadataServiceServer
}

func (leaderlessMetadataServer) CheckBucketExists(context.Context, *castorv1.CheckBucketExistsRequest) (*castorv1.CheckBucketExistsResponse, error) {
	return nil, status.Error(codes.Unavailable, "raft leader unavailable")
}

func TestHeadBucket_NoMetadataLeaderReturns503(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	castorv1.RegisterMetadataServiceServer(srv, leaderlessMetadataServer{})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	engine := storage.New(config.Config{
		MetadataNodes:      []string{lis.Addr().String()},
		MetadataLeaderWait: 200 * time.Millisecond,
	}, nil)
	defer engine.Close()
	be := New(engine, nil)

	_, err = be.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: aws.String("any-bucket")})
	var apiErr s3err.APIError
	if !errors.As(err, &apiErr) || apiErr.HTTPStatusCode != http.StatusServiceUnavailable {
		t.Fatalf("HeadBucket error = %v, want a 503 S3 error", err)
	}
}
