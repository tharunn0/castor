package heartbeat

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/data/config"
	"github.com/tharunn0/castor/internal/data/storage"
	"github.com/tharunn0/castor/internal/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type DiskStatsProvider interface {
	GetDiskStats() (storage.DiskStats, error)
}

type DialFunc func(ctx context.Context, target string) (*grpc.ClientConn, castorv1.MetadataServiceClient, error)

type Option func(*Syncer)

func WithLogger(logger *slog.Logger) Option {
	return func(s *Syncer) {
		s.logger = logger
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(s *Syncer) {
		if timeout > 0 {
			s.timeout = timeout
		}
	}
}

func WithDialFunc(fn DialFunc) Option {
	return func(s *Syncer) {
		if fn != nil {
			s.dialFunc = fn
		}
	}
}

type Syncer struct {
	cfg      config.NodeConfig
	store    DiskStatsProvider
	logger   *slog.Logger
	timeout  time.Duration
	dialFunc DialFunc

	mu          sync.Mutex
	currentAddr string
	client      castorv1.MetadataServiceClient
	conn        *grpc.ClientConn
	seedIdx     int
	cancel      context.CancelFunc
	done        chan struct{}
}

func NewSyncer(cfg config.NodeConfig, store DiskStatsProvider, opts ...Option) *Syncer {
	s := &Syncer{
		cfg:      cfg,
		store:    store,
		logger:   slog.Default(),
		timeout:  2 * time.Second,
		dialFunc: defaultDial,
		done:     make(chan struct{}),
	}
	if len(cfg.MetadataNodes) > 0 {
		s.currentAddr = cfg.MetadataNodes[0]
	} else if cfg.MetadataAddr != "" {
		s.currentAddr = cfg.MetadataAddr
	} else {
		s.currentAddr = "127.0.0.1:9090"
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func defaultDial(ctx context.Context, target string) (*grpc.ClientConn, castorv1.MetadataServiceClient, error) {
	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		telemetry.ClientDialOption(),
	)
	if err != nil {
		return nil, nil, err
	}

	return conn, castorv1.NewMetadataServiceClient(conn), nil
}

func ExtractLeaderHint(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if s, ok := status.FromError(err); ok {
		msg = s.Message()
	}
	const prefix = "leader is "
	idx := strings.Index(msg, prefix)
	if idx == -1 {
		return ""
	}
	hint := strings.TrimSpace(msg[idx+len(prefix):])
	if end := strings.IndexAny(hint, " \t\r\n,"); end != -1 {
		hint = hint[:end]
	}
	return hint
}

func IsNotLeaderError(err error) bool {
	if err == nil {
		return false
	}
	if s, ok := status.FromError(err); ok {
		msg := s.Message()
		if s.Code() == codes.Unavailable {
			return true
		}
		return strings.Contains(msg, "not the raft leader") || strings.Contains(msg, "raft leader unavailable")
	}
	msg := err.Error()
	return strings.Contains(msg, "not the raft leader") || strings.Contains(msg, "raft leader unavailable")
}

func ResolveLeaderAddress(hint string, seeds []string) string {
	if hint == "" {
		return ""
	}
	for _, seed := range seeds {
		if seed == hint {
			return seed
		}
	}

	hintHost, hintPort, err := net.SplitHostPort(hint)
	if err != nil {
		hintHost = hint
	}

	for _, seed := range seeds {
		seedHost, _, err := net.SplitHostPort(seed)
		if err == nil && seedHost == hintHost {
			if len(seeds) == 1 || hintPort == "" {
				return seed
			}
		}
	}

	if hintPort != "" {
		if raftPortNum, err := strconv.Atoi(hintPort); err == nil {
			possibleGrpcPort := strconv.Itoa(raftPortNum - 1)
			candidate := net.JoinHostPort(hintHost, possibleGrpcPort)
			for _, seed := range seeds {
				if seed == candidate {
					return seed
				}
			}
			return candidate
		}
	}

	return hint
}

func (s *Syncer) CurrentAddr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentAddr
}

func (s *Syncer) getClient(ctx context.Context) (castorv1.MetadataServiceClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.client != nil {
		return s.client, nil
	}

	conn, client, err := s.dialFunc(ctx, s.currentAddr)
	if err != nil {
		return nil, fmt.Errorf("dial metadata service %s: %w", s.currentAddr, err)
	}

	s.conn = conn
	s.client = client
	return s.client, nil
}

func (s *Syncer) switchToAddr(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
	s.client = nil
	s.currentAddr = addr
}

func (s *Syncer) rotateSeed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
	s.client = nil
	if len(s.cfg.MetadataNodes) == 0 {
		return
	}
	s.seedIdx = (s.seedIdx + 1) % len(s.cfg.MetadataNodes)
	s.currentAddr = s.cfg.MetadataNodes[s.seedIdx]
}

func (s *Syncer) executeWithLeader(ctx context.Context, op func(client castorv1.MetadataServiceClient) error) error {

	maxAttempts := max(len(s.cfg.MetadataNodes)+2, 3)

	var lastErr error
	for range maxAttempts {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		client, err := s.getClient(ctx)
		if err != nil {
			lastErr = err
			s.rotateSeed()
			continue
		}

		err = op(client)
		if err == nil {
			return nil
		}
		lastErr = err

		if IsNotLeaderError(err) {
			hint := ExtractLeaderHint(err)
			target := ResolveLeaderAddress(hint, s.cfg.MetadataNodes)
			if target != "" && target != s.CurrentAddr() {
				s.logger.Info("redirecting heartbeat to metadata leader", "from", s.CurrentAddr(), "leader", target)
				s.switchToAddr(target)
				continue
			}
			s.rotateSeed()
			continue
		}

		// On connection or network error, rotate to next seed node.
		s.rotateSeed()
	}

	return lastErr
}

func (s *Syncer) Register(ctx context.Context) error {
	var total, free int64
	if s.store != nil {
		if stats, err := s.store.GetDiskStats(); err == nil {
			total = int64(stats.TotalBytes)
			free = int64(stats.FreeBytes)
		}
	}

	req := &castorv1.RegisterNodeRequest{
		NodeId:      s.cfg.NodeId,
		GrpcAddress: s.cfg.AdvertiseAddr,
		TotalBytes:  total,
		FreeBytes:   free,
	}

	return s.executeWithLeader(ctx, func(client castorv1.MetadataServiceClient) error {
		callCtx, cancel := context.WithTimeout(ctx, s.timeout)
		defer cancel()
		_, err := client.RegisterNode(callCtx, req)
		return err
	})
}

func (s *Syncer) SendHeartbeat(ctx context.Context) error {
	var total, free int64
	if s.store != nil {
		if stats, err := s.store.GetDiskStats(); err == nil {
			total = int64(stats.TotalBytes)
			free = int64(stats.FreeBytes)
		}
	}

	req := &castorv1.HeartbeatRequest{
		NodeId:      s.cfg.NodeId,
		GrpcAddress: s.cfg.AdvertiseAddr,
		TotalBytes:  total,
		FreeBytes:   free,
		Timestamp:   timestamppb.Now(),
	}

	return s.executeWithLeader(ctx, func(client castorv1.MetadataServiceClient) error {
		callCtx, cancel := context.WithTimeout(ctx, s.timeout)
		defer cancel()
		_, err := client.Heartbeat(callCtx, req)
		return err
	})
}

func (s *Syncer) Start(ctx context.Context) {
	s.mu.Lock()
	if s.cancel != nil {
		s.mu.Unlock()
		return
	}
	loopCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.done = make(chan struct{})
	s.mu.Unlock()

	// Initial registration is best-effort; subsequent heartbeats auto-register in the metadata registry.
	if err := s.Register(loopCtx); err != nil {
		s.logger.Warn("initial node registration failed, will retry on heartbeat",
			"node_id", s.cfg.NodeId,
			"metadata_addr", s.CurrentAddr(),
			"error", err,
		)
	}

	go func() {
		defer close(s.done)
		ticker := time.NewTicker(s.cfg.HeartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
				if err := s.SendHeartbeat(loopCtx); err != nil && !errors.Is(err, context.Canceled) {
					s.logger.Warn("node heartbeat failed",
						"node_id", s.cfg.NodeId,
						"metadata_addr", s.CurrentAddr(),
						"error", err,
					)
				}
			}
		}
	}()
}

func (s *Syncer) Stop() error {
	s.mu.Lock()
	var done chan struct{}
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
		done = s.done
	}
	s.mu.Unlock()

	if done != nil {
		<-done
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		err := s.conn.Close()
		s.conn = nil
		s.client = nil
		return err
	}
	return nil
}
