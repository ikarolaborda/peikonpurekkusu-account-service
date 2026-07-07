// Package account exposes build-time assets embedded from the module root
// (go:embed patterns are relative to the file that declares them).
package account

import "embed"

//go:embed migrations/*.sql
var MigrationsFS embed.FS
