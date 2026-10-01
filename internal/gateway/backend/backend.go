package backend

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/gateway/storage"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CastorBackend struct {
	backend.BackendUnsupported
	engine     *storage.StorageEngine
	metaClient castorv1.MetadataServiceClient
}

func New(engine *storage.StorageEngine, metaClient castorv1.MetadataServiceClient) *CastorBackend {
	return &CastorBackend{
		engine:     engine,
		metaClient: metaClient,
	}
}

func (b *CastorBackend) String() string {
	return "castor"
}

func (b *CastorBackend) Shutdown() {}

func (b *CastorBackend) NormalizeObjectKey(bucket, object string) string {
	return object
}

func (b *CastorBackend) GetBucketAcl(ctx context.Context, input *s3.GetBucketAclInput) ([]byte, error) {
	return []byte(`{"Owner":"admin","Grants":[]}`), nil
}

func (b *CastorBackend) GetBucketVersioning(ctx context.Context, bucket string) (s3response.GetBucketVersioningOutput, error) {
	return s3response.GetBucketVersioningOutput{}, nil
}

func (b *CastorBackend) GetObjectLockConfiguration(ctx context.Context, bucket string) ([]byte, error) {
	return nil, s3err.GetAPIError(s3err.ErrObjectLockConfigurationNotFound)
}

func (b *CastorBackend) HeadBucket(ctx context.Context, input *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	if input == nil || input.Bucket == nil || *input.Bucket == "" {
		return nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	exists, err := b.metaClient.CheckBucketExists(ctx, &castorv1.CheckBucketExistsRequest{Bucket: *input.Bucket})
	if err != nil {
		return nil, err
	}
	if !exists.GetExists() {
		return nil, s3err.GetBucketErr(s3err.ErrNoSuchBucket, *input.Bucket)
	}
	return &s3.HeadBucketOutput{}, nil
}

func (b *CastorBackend) PutObject(ctx context.Context, input s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	if input.Bucket == nil || *input.Bucket == "" {
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if input.Key == nil || *input.Key == "" {
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	var size int64
	if input.ContentLength != nil {
		size = *input.ContentLength
	}

	etag, err := b.engine.PutObject(ctx, *input.Bucket, *input.Key, "admin", input.Body, size)
	if err != nil {
		if errors.Is(err, storage.ErrBucketNotFound) {
			return s3response.PutObjectOutput{}, s3err.GetBucketErr(s3err.ErrNoSuchBucket, *input.Bucket)
		}
		if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
			return s3response.PutObjectOutput{}, s3err.GetBucketErr(s3err.ErrNoSuchBucket, *input.Bucket)
		}
		return s3response.PutObjectOutput{}, err
	}

	quotedETag := fmt.Sprintf("%q", etag)
	out := s3response.PutObjectOutput{
		ETag: quotedETag,
	}
	if input.ContentLength != nil {
		out.Size = &size
	}
	return out, nil
}
