package main

import (
	"fmt"

	"github.com/tharunn0/castor/internal/version"
)

func main() {
	fmt.Print(version.Version, " ", version.Commit)
}
