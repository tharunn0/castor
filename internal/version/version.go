// Package version holds build information injected at link time via
// -ldflags "-X github.com/tharunn0/castor/internal/version.Version=...".
package version

var (
	Version = "dev"
	Commit  = "unknown"
)
