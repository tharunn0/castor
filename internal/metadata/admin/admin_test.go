package admin

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tharunn0/castor/internal/metadata/consensus"
)

type mockRaftStatusProvider struct {
	status consensus.RaftStatus
}

func (m *mockRaftStatusProvider) Status() consensus.RaftStatus {
	return m.status
}

func TestAdminServer_RaftStatus(t *testing.T) {
	expected := consensus.RaftStatus{
		NodeID:            "metadata-1",
		Role:              "LEADER",
		Term:              3,
		LeaderID:          "metadata-1",
		LeaderAddr:        "127.0.0.1:9091",
		CommitIndex:       42,
		AppliedIndex:      42,
		LastLogIndex:      43,
		LastSnapshotIndex: 10,
		Peers: []consensus.RaftPeerStatus{
			{
				NodeID:   "metadata-2",
				Address:  "127.0.0.1:9092",
				Suffrage: "Voter",
			},
		},
	}

	provider := &mockRaftStatusProvider{status: expected}
	srv := New(":0", provider)

	req := httptest.NewRequest(http.MethodGet, "/admin/raft/status", nil)
	resp, err := srv.App().Test(req)
	if err != nil {
		t.Fatalf("failed to execute test request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200 OK, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	var actual consensus.RaftStatus
	if err := json.Unmarshal(body, &actual); err != nil {
		t.Fatalf("failed to unmarshal JSON response: %v", err)
	}

	if actual.NodeID != expected.NodeID {
		t.Errorf("NodeID: expected %s, got %s", expected.NodeID, actual.NodeID)
	}
	if actual.Role != expected.Role {
		t.Errorf("Role: expected %s, got %s", expected.Role, actual.Role)
	}
	if actual.Term != expected.Term {
		t.Errorf("Term: expected %d, got %d", expected.Term, actual.Term)
	}
	if actual.LeaderID != expected.LeaderID {
		t.Errorf("LeaderID: expected %s, got %s", expected.LeaderID, actual.LeaderID)
	}
	if actual.LeaderAddr != expected.LeaderAddr {
		t.Errorf("LeaderAddr: expected %s, got %s", expected.LeaderAddr, actual.LeaderAddr)
	}
	if actual.CommitIndex != expected.CommitIndex {
		t.Errorf("CommitIndex: expected %d, got %d", expected.CommitIndex, actual.CommitIndex)
	}
	if actual.AppliedIndex != expected.AppliedIndex {
		t.Errorf("AppliedIndex: expected %d, got %d", expected.AppliedIndex, actual.AppliedIndex)
	}
	if actual.LastLogIndex != expected.LastLogIndex {
		t.Errorf("LastLogIndex: expected %d, got %d", expected.LastLogIndex, actual.LastLogIndex)
	}
	if actual.LastSnapshotIndex != expected.LastSnapshotIndex {
		t.Errorf("LastSnapshotIndex: expected %d, got %d", expected.LastSnapshotIndex, actual.LastSnapshotIndex)
	}
	if len(actual.Peers) != 1 || actual.Peers[0].NodeID != "metadata-2" {
		t.Errorf("Peers mismatch: %+v", actual.Peers)
	}
}

func TestAdminServer_RaftStatus_NilProvider(t *testing.T) {
	srv := New(":0", nil)

	req := httptest.NewRequest(http.MethodGet, "/admin/raft/status", nil)
	resp, err := srv.App().Test(req)
	if err != nil {
		t.Fatalf("failed to execute test request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503 Service Unavailable, got %d", resp.StatusCode)
	}
}

func TestAdminServer_Healthz(t *testing.T) {
	srv := New(":0", nil)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	resp, err := srv.App().Test(req)
	if err != nil {
		t.Fatalf("failed to execute test request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200 OK, got %d", resp.StatusCode)
	}
}
