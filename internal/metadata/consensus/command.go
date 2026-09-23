package consensus

import (
	"errors"
	"fmt"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"google.golang.org/protobuf/proto"
)

var (
	ErrEmptyCommand   = errors.New("command payload is empty")
	ErrUnknownCommand = errors.New("unknown command type")
	ErrTypeMismatch   = errors.New("command type mismatch")
)

// CommandType defines the operation type replicated via Raft log.
type CommandType uint8

const (
	CmdUnknown CommandType = iota
	CmdCreateBucket
	CmdDeleteBucket
	CmdCommitManifest
	CmdDeleteManifest
	CmdInitiateMultipart
	CmdCommitPart
	CmdCompleteMultipart
	CmdAbortMultipart
	CmdUpdateChunkLocation
	CmdRemoveChunkLocations
)

func (c CommandType) String() string {
	switch c {
	case CmdCreateBucket:
		return "CREATE_BUCKET"
	case CmdDeleteBucket:
		return "DELETE_BUCKET"
	case CmdCommitManifest:
		return "COMMIT_MANIFEST"
	case CmdDeleteManifest:
		return "DELETE_MANIFEST"
	case CmdInitiateMultipart:
		return "INITIATE_MULTIPART"
	case CmdCommitPart:
		return "COMMIT_PART"
	case CmdCompleteMultipart:
		return "COMPLETE_MULTIPART"
	case CmdAbortMultipart:
		return "ABORT_MULTIPART"
	case CmdUpdateChunkLocation:
		return "UPDATE_CHUNK_LOCATION"
	case CmdRemoveChunkLocations:
		return "REMOVE_CHUNK_LOCATIONS"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", c)
	}
}

// Command is the envelope replicated through Raft consensus.
// Wire format: [1 byte: CommandType][N bytes: Protobuf Payload]
type Command struct {
	Type    CommandType
	Payload []byte
}

// Encode serializes the command into wire format.
func (c *Command) Encode() ([]byte, error) {
	if c.Type == CmdUnknown {
		return nil, ErrUnknownCommand
	}
	buf := make([]byte, 1+len(c.Payload))
	buf[0] = byte(c.Type)
	copy(buf[1:], c.Payload)
	return buf, nil
}

// DecodeCommand deserializes wire format into a Command.
func DecodeCommand(data []byte) (*Command, error) {
	if len(data) == 0 {
		return nil, ErrEmptyCommand
	}
	cmdType := CommandType(data[0])
	if cmdType == CmdUnknown || cmdType > CmdRemoveChunkLocations {
		return nil, fmt.Errorf("%w: %d", ErrUnknownCommand, cmdType)
	}

	payload := make([]byte, len(data)-1)
	copy(payload, data[1:])

	return &Command{
		Type:    cmdType,
		Payload: payload,
	}, nil
}

func NewCreateBucketCommand(req *castorv1.CreateBucketMetadataRequest) (*Command, error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	return &Command{Type: CmdCreateBucket, Payload: data}, nil
}

func NewDeleteBucketCommand(req *castorv1.DeleteBucketMetadataRequest) (*Command, error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	return &Command{Type: CmdDeleteBucket, Payload: data}, nil
}

func NewCommitManifestCommand(req *castorv1.CommitManifestRequest) (*Command, error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	return &Command{Type: CmdCommitManifest, Payload: data}, nil
}

func NewDeleteManifestCommand(req *castorv1.DeleteManifestRequest) (*Command, error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	return &Command{Type: CmdDeleteManifest, Payload: data}, nil
}

func NewInitiateMultipartCommand(req *castorv1.InitiateMultipartMetaRequest) (*Command, error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	return &Command{Type: CmdInitiateMultipart, Payload: data}, nil
}

func NewCommitPartCommand(req *castorv1.CommitPartMetaRequest) (*Command, error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	return &Command{Type: CmdCommitPart, Payload: data}, nil
}

func NewCompleteMultipartCommand(req *castorv1.CompleteMultipartMetaRequest) (*Command, error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	return &Command{Type: CmdCompleteMultipart, Payload: data}, nil
}

func NewAbortMultipartCommand(req *castorv1.AbortMultipartMetaRequest) (*Command, error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	return &Command{Type: CmdAbortMultipart, Payload: data}, nil
}

func NewUpdateChunkLocationCommand(req *castorv1.UpdateChunkLocationRequest) (*Command, error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	return &Command{Type: CmdUpdateChunkLocation, Payload: data}, nil
}

func NewRemoveChunkLocationsCommand(req *castorv1.RemoveChunkLocationsRequest) (*Command, error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	return &Command{Type: CmdRemoveChunkLocations, Payload: data}, nil
}

// Payload decoders

func (c *Command) DecodeCreateBucket() (*castorv1.CreateBucketMetadataRequest, error) {
	if c.Type != CmdCreateBucket {
		return nil, fmt.Errorf("%w: expected %s got %s", ErrTypeMismatch, CmdCreateBucket, c.Type)
	}
	req := &castorv1.CreateBucketMetadataRequest{}
	if err := proto.Unmarshal(c.Payload, req); err != nil {
		return nil, err
	}
	return req, nil
}

func (c *Command) DecodeDeleteBucket() (*castorv1.DeleteBucketMetadataRequest, error) {
	if c.Type != CmdDeleteBucket {
		return nil, fmt.Errorf("%w: expected %s got %s", ErrTypeMismatch, CmdDeleteBucket, c.Type)
	}
	req := &castorv1.DeleteBucketMetadataRequest{}
	if err := proto.Unmarshal(c.Payload, req); err != nil {
		return nil, err
	}
	return req, nil
}

func (c *Command) DecodeCommitManifest() (*castorv1.CommitManifestRequest, error) {
	if c.Type != CmdCommitManifest {
		return nil, fmt.Errorf("%w: expected %s got %s", ErrTypeMismatch, CmdCommitManifest, c.Type)
	}
	req := &castorv1.CommitManifestRequest{}
	if err := proto.Unmarshal(c.Payload, req); err != nil {
		return nil, err
	}
	return req, nil
}

func (c *Command) DecodeDeleteManifest() (*castorv1.DeleteManifestRequest, error) {
	if c.Type != CmdDeleteManifest {
		return nil, fmt.Errorf("%w: expected %s got %s", ErrTypeMismatch, CmdDeleteManifest, c.Type)
	}
	req := &castorv1.DeleteManifestRequest{}
	if err := proto.Unmarshal(c.Payload, req); err != nil {
		return nil, err
	}
	return req, nil
}

func (c *Command) DecodeInitiateMultipart() (*castorv1.InitiateMultipartMetaRequest, error) {
	if c.Type != CmdInitiateMultipart {
		return nil, fmt.Errorf("%w: expected %s got %s", ErrTypeMismatch, CmdInitiateMultipart, c.Type)
	}
	req := &castorv1.InitiateMultipartMetaRequest{}
	if err := proto.Unmarshal(c.Payload, req); err != nil {
		return nil, err
	}
	return req, nil
}

func (c *Command) DecodeCommitPart() (*castorv1.CommitPartMetaRequest, error) {
	if c.Type != CmdCommitPart {
		return nil, fmt.Errorf("%w: expected %s got %s", ErrTypeMismatch, CmdCommitPart, c.Type)
	}
	req := &castorv1.CommitPartMetaRequest{}
	if err := proto.Unmarshal(c.Payload, req); err != nil {
		return nil, err
	}
	return req, nil
}

func (c *Command) DecodeCompleteMultipart() (*castorv1.CompleteMultipartMetaRequest, error) {
	if c.Type != CmdCompleteMultipart {
		return nil, fmt.Errorf("%w: expected %s got %s", ErrTypeMismatch, CmdCompleteMultipart, c.Type)
	}
	req := &castorv1.CompleteMultipartMetaRequest{}
	if err := proto.Unmarshal(c.Payload, req); err != nil {
		return nil, err
	}
	return req, nil
}

func (c *Command) DecodeAbortMultipart() (*castorv1.AbortMultipartMetaRequest, error) {
	if c.Type != CmdAbortMultipart {
		return nil, fmt.Errorf("%w: expected %s got %s", ErrTypeMismatch, CmdAbortMultipart, c.Type)
	}
	req := &castorv1.AbortMultipartMetaRequest{}
	if err := proto.Unmarshal(c.Payload, req); err != nil {
		return nil, err
	}
	return req, nil
}

func (c *Command) DecodeUpdateChunkLocation() (*castorv1.UpdateChunkLocationRequest, error) {
	if c.Type != CmdUpdateChunkLocation {
		return nil, fmt.Errorf("%w: expected %s got %s", ErrTypeMismatch, CmdUpdateChunkLocation, c.Type)
	}
	req := &castorv1.UpdateChunkLocationRequest{}
	if err := proto.Unmarshal(c.Payload, req); err != nil {
		return nil, err
	}
	return req, nil
}

func (c *Command) DecodeRemoveChunkLocations() (*castorv1.RemoveChunkLocationsRequest, error) {
	if c.Type != CmdRemoveChunkLocations {
		return nil, fmt.Errorf("%w: expected %s got %s", ErrTypeMismatch, CmdRemoveChunkLocations, c.Type)
	}
	req := &castorv1.RemoveChunkLocationsRequest{}
	if err := proto.Unmarshal(c.Payload, req); err != nil {
		return nil, err
	}
	return req, nil
}
