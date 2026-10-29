package version

import (
	"os/exec"
	"path/filepath"
	"testing"
)

func TestDefaults(t *testing.T) {
	if Version != "dev" {
		t.Errorf("Version = %q, want %q", Version, "dev")
	}
	if Commit != "unknown" {
		t.Errorf("Commit = %q, want %q", Commit, "unknown")
	}
}

func TestLdflagsInjection(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a binary")
	}
	bin := filepath.Join(t.TempDir(), "printversion")
	pkg := "github.com/tharunn0/castor/internal/version"
	ldflags := "-X " + pkg + ".Version=v1.2.3 -X " + pkg + ".Commit=abc1234"

	build := exec.Command("go", "build", "-ldflags", ldflags, "-o", bin, "./testdata/printversion")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	out, err := exec.Command(bin).Output()
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got, want := string(out), "v1.2.3 abc1234"; got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
}
