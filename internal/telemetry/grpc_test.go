package telemetry

import (
	"testing"
)

func TestGRPCOptions(t *testing.T) {
	clientOpt := ClientDialOption()
	if clientOpt == nil {
		t.Fatal("expected non-nil client dial option")
	}

	serverOpt := ServerOption()
	if serverOpt == nil {
		t.Fatal("expected non-nil server option")
	}
}
