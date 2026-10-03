package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
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

var _ backend.Backend = (*CastorBackend)(nil)

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

func (b *CastorBackend) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if input == nil || input.Bucket == nil || *input.Bucket == "" {
		return nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if input.Key == nil || *input.Key == "" {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	var start, length int64
	var isRangeValid bool

	if input.Range != nil && *input.Range != "" {
		exists, err := b.metaClient.CheckBucketExists(ctx, &castorv1.CheckBucketExistsRequest{Bucket: *input.Bucket})
		if err != nil {
			if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
				return nil, s3err.GetBucketErr(s3err.ErrNoSuchBucket, *input.Bucket)
			}
			return nil, err
		}
		if !exists.GetExists() {
			return nil, s3err.GetBucketErr(s3err.ErrNoSuchBucket, *input.Bucket)
		}

		manifest, err := b.metaClient.GetManifest(ctx, &castorv1.GetManifestRequest{
			Bucket: *input.Bucket,
			Key:    *input.Key,
		})
		if err != nil {
			if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
				return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
			}
			return nil, err
		}
		if manifest.GetStatus() == "deleted" {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}

		s, l, valid, rangeErr := backend.ParseObjectRange(manifest.GetSize(), *input.Range)
		if rangeErr != nil {
			return nil, rangeErr
		}
		start, length, isRangeValid = s, l, valid
	}

	var reader io.ReadCloser
	var info *storage.ObjectInfo
	var err error

	if isRangeValid {
		reader, info, err = b.engine.GetObjectRange(ctx, *input.Bucket, *input.Key, start, length)
	} else {
		reader, info, err = b.engine.GetObject(ctx, *input.Bucket, *input.Key)
	}
	if err != nil {
		if errors.Is(err, storage.ErrBucketNotFound) {
			return nil, s3err.GetBucketErr(s3err.ErrNoSuchBucket, *input.Bucket)
		}
		if errors.Is(err, storage.ErrObjectNotFound) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		return nil, err
	}

	if preErr := backend.EvaluatePreconditions(info.ETag, info.UpdatedAt, backend.PreConditions{
		IfMatch:       input.IfMatch,
		IfNoneMatch:   input.IfNoneMatch,
		IfModSince:    input.IfModifiedSince,
		IfUnmodeSince: input.IfUnmodifiedSince,
	}); preErr != nil {
		_ = reader.Close()
		return nil, preErr
	}

	out := &s3.GetObjectOutput{
		AcceptRanges: aws.String("bytes"),
		Body:         reader,
		ETag:         aws.String(fmt.Sprintf("%q", info.ETag)),
		LastModified: aws.Time(info.UpdatedAt),
	}
	if isRangeValid {
		out.ContentRange = aws.String(fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, info.Size))
		out.ContentLength = aws.Int64(length)
	} else {
		out.ContentLength = aws.Int64(info.Size)
	}
	if info.ContentType != "" {
		out.ContentType = aws.String(info.ContentType)
	} else {
		out.ContentType = aws.String("application/octet-stream")
	}

	return out, nil
}

func (b *CastorBackend) CreateBucket(ctx context.Context, req *s3.CreateBucketInput, defaultACL []byte) error {
	if req == nil || req.Bucket == nil || *req.Bucket == "" {
		return s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}

	ownerID := "admin"
	if len(defaultACL) > 0 {
		var acl struct {
			Owner string `json:"Owner"`
		}
		if err := json.Unmarshal(defaultACL, &acl); err == nil && acl.Owner != "" {
			ownerID = acl.Owner
		}
	}

	var tags map[string]string
	if req.CreateBucketConfiguration != nil && len(req.CreateBucketConfiguration.Tags) > 0 {
		tags = make(map[string]string, len(req.CreateBucketConfiguration.Tags))
		for _, tag := range req.CreateBucketConfiguration.Tags {
			if tag.Key != nil && tag.Value != nil {
				tags[*tag.Key] = *tag.Value
			}
		}
	}

	err := b.engine.CreateBucket(ctx, *req.Bucket, ownerID, tags)
	if err != nil {
		if errors.Is(err, storage.ErrBucketAlreadyExists) {
			return s3err.GetBucketErr(s3err.ErrBucketAlreadyOwnedByYou, *req.Bucket)
		}
		if s, ok := status.FromError(err); ok && s.Code() == codes.AlreadyExists {
			return s3err.GetBucketErr(s3err.ErrBucketAlreadyOwnedByYou, *req.Bucket)
		}
		return err
	}

	return nil
}
