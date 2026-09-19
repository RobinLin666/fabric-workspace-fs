// Command live-smoke is an explicitly authorized, disposable LIVE fixture test.
// It is not an implementation of notebook creation/deletion in the filesystem.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/transport"
)

const (
	runTimeout       = 15 * time.Minute
	cleanupTimeout   = 5 * time.Minute
	operationTimeout = 5 * time.Minute
	cacheTTL         = 5 * time.Second // Deliberately shorter than the two-minute product default.
	maxNotebookBytes = 1 << 20
)

type options struct {
	Workspace string
	Lakehouse string
	Evidence  string
}

type problem struct {
	message string
	code    int
}

func (p *problem) Error() string { return p.message }

func fail(message string) error { return &problem{message: message, code: 1} }

func usageError(message string) error { return &problem{message: message, code: 2} }

func parseOptions(args []string) (options, error) {
	var opts options
	var allow bool
	flags := flag.NewFlagSet("live-smoke", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.BoolVar(&allow, "allow-writes", false, "authorize disposable live fixture writes")
	flags.StringVar(&opts.Workspace, "workspace", "", "selected workspace UUID")
	flags.StringVar(&opts.Lakehouse, "lakehouse", "", "selected Lakehouse UUID")
	flags.StringVar(&opts.Evidence, "evidence", "", "NEW durable evidence JSON file")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return options{}, usageError("invalid flags; no authentication or remote operation was attempted")
	}
	if !allow || opts.Workspace == "" || opts.Lakehouse == "" || strings.TrimSpace(opts.Evidence) == "" {
		return options{}, usageError("all four flags are required, including --allow-writes")
	}
	if fabric.ValidateID(opts.Workspace) != nil || fabric.ValidateID(opts.Lakehouse) != nil {
		return options{}, usageError("--workspace and --lakehouse must be canonical UUIDs")
	}
	if opts.Evidence != strings.TrimSpace(opts.Evidence) || strings.ContainsRune(opts.Evidence, 0) ||
		filepath.Base(opts.Evidence) == "." || filepath.Base(opts.Evidence) == string(filepath.Separator) ||
		strings.HasSuffix(opts.Evidence, string(filepath.Separator)) {
		return options{}, usageError("--evidence must name a NEW file in an existing directory")
	}
	absolute, err := filepath.Abs(opts.Evidence)
	if err != nil {
		return options{}, usageError("invalid evidence file path")
	}
	opts.Workspace = strings.ToLower(opts.Workspace)
	opts.Lakehouse = strings.ToLower(opts.Lakehouse)
	opts.Evidence = absolute
	return opts, nil
}

// Never format arbitrary SDK, JSON, command, or service error strings. In
// particular HTTPError.Error, not its fields or body, is the safe HTTP surface.
func safeError(err error) string {
	if err == nil {
		return ""
	}
	var httpErr *transport.HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.Error()
	}
	var own *problem
	if errors.As(err, &own) {
		return own.message
	}
	return safeSystemError(err)
}

func execute(args []string, stdout, stderr io.Writer) int {
	opts, err := parseOptions(args)
	if err == nil {
		err = run(opts, stdout)
	}
	if err == nil {
		fmt.Fprintln(stdout, "live smoke passed; fixture cleanup is recorded in the evidence JSON")
		return 0
	}
	fmt.Fprintln(stderr, safeError(err))
	var own *problem
	if errors.As(err, &own) && own.code == 2 {
		fmt.Fprintln(stderr, "usage: live-smoke --allow-writes --workspace UUID --lakehouse UUID --evidence NEW_FILE.json")
		return 2
	}
	return 1
}

func main() {
	prepareSignals()
	code := execute(os.Args[1:], os.Stdout, os.Stderr)
	if code != 0 {
		os.Exit(code)
	}
}
