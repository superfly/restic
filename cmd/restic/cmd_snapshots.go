package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui"
	"github.com/restic/restic/internal/ui/progress"
	"github.com/restic/restic/internal/ui/table"
	"github.com/restic/restic/internal/ui/termstatus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
)

func newSnapshotsCommand() *cobra.Command {
	var opts SnapshotOptions

	cmd := &cobra.Command{
		Use:   "snapshots [flags] [snapshotID ...]",
		Short: "List all snapshots",
		Long: `
The "snapshots" command lists all snapshots stored in the repository.

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
Exit status is 10 if the repository does not exist.
Exit status is 11 if the repository is already locked.
Exit status is 12 if the password is incorrect.
`,
		GroupID:           cmdGroupDefault,
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			term, cancel := setupTermstatus()
			defer cancel()
			return runSnapshots(cmd.Context(), opts, globalOptions, args, term)
		},
	}

	opts.AddFlags(cmd.Flags())
	return cmd
}

// SnapshotOptions bundles all options for the snapshots command.
type SnapshotOptions struct {
	restic.SnapshotFilter
	Compact bool
	Last    bool // This option should be removed in favour of Latest.
	Latest  int
	GroupBy restic.SnapshotGroupByOptions
	Usage   bool
}

func (opts *SnapshotOptions) AddFlags(f *pflag.FlagSet) {
	initMultiSnapshotFilter(f, &opts.SnapshotFilter, true)
	f.BoolVarP(&opts.Compact, "compact", "c", false, "use compact output format")
	f.BoolVar(&opts.Last, "last", false, "only show the last snapshot for each host and path")
	err := f.MarkDeprecated("last", "use --latest 1")
	if err != nil {
		// MarkDeprecated only returns an error when the flag is not found
		panic(err)
	}
	f.IntVar(&opts.Latest, "latest", 0, "only show the last `n` snapshots for each host and path")
	f.VarP(&opts.GroupBy, "group-by", "g", "`group` snapshots by host, paths and/or tags, separated by comma")
	f.BoolVar(&opts.Usage, "usage", false, "show storage usage per snapshot. Deduplicated data is attributed to the oldest snapshot that includes it")
}

func runSnapshots(ctx context.Context, opts SnapshotOptions, gopts GlobalOptions, args []string, term *termstatus.Terminal) error {
	verbosity := gopts.verbosity
	if gopts.JSON {
		verbosity = 0
	}
	printer := newTerminalProgressPrinter(verbosity, term)

	ctx, repo, unlock, err := openWithReadLock(ctx, gopts, gopts.NoLock)
	if err != nil {
		return err
	}
	defer unlock()

	var snapshots restic.Snapshots
	for sn := range FindFilteredSnapshots(ctx, repo, repo, &opts.SnapshotFilter, args) {
		snapshots = append(snapshots, sn)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	snapshotGroups, grouped, err := restic.GroupSnapshots(snapshots, opts.GroupBy)
	if err != nil {
		return err
	}

	for k, list := range snapshotGroups {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if opts.Last {
			// This branch should be removed in the same time
			// that --last.
			list = FilterLatestSnapshots(list, 1)
		} else if opts.Latest > 0 {
			list = FilterLatestSnapshots(list, opts.Latest)
		}
		sort.Sort(sort.Reverse(list))
		snapshotGroups[k] = list
	}

	var usage map[restic.ID]uint64
	if opts.Usage {
		bar := newIndexTerminalProgress(gopts.Quiet, gopts.JSON, term)
		if err := repo.LoadIndex(ctx, bar); err != nil {
			return err
		}

		printer.P("calculating usage for %d snapshots", len(snapshots))
		bar = printer.NewCounter("snapshots")
		bar.SetMax(uint64(len(snapshots)))
		usage, err = calculateSnapshotsUsage(ctx, repo, snapshots, bar)
		bar.Done()
		if err != nil {
			return fmt.Errorf("calculating snapshot usage: %w", err)
		}
	}

	if gopts.JSON {
		err := printSnapshotGroupJSON(globalOptions.stdout, snapshotGroups, grouped)
		if err != nil {
			Warnf("error printing snapshots: %v\n", err)
		}
		return nil
	}

	for k, list := range snapshotGroups {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if grouped {
			err := PrintSnapshotGroupHeader(globalOptions.stdout, k)
			if err != nil {
				Warnf("error printing snapshots: %v\n", err)
				return nil
			}
		}
		PrintSnapshots(globalOptions.stdout, list, nil, usage, opts.Compact)
	}

	return nil
}

// filterLastSnapshotsKey is used by FilterLastSnapshots.
type filterLastSnapshotsKey struct {
	Hostname    string
	JoinedPaths string
}

// newFilterLastSnapshotsKey initializes a filterLastSnapshotsKey from a Snapshot
func newFilterLastSnapshotsKey(sn *restic.Snapshot) filterLastSnapshotsKey {
	// Shallow slice copy
	var paths = make([]string, len(sn.Paths))
	copy(paths, sn.Paths)
	sort.Strings(paths)
	return filterLastSnapshotsKey{sn.Hostname, strings.Join(paths, "|")}
}

// FilterLatestSnapshots filters a list of snapshots to only return
// the limit last entries for each hostname and path. If the snapshot
// contains multiple paths, they will be joined and treated as one
// item.
func FilterLatestSnapshots(list restic.Snapshots, limit int) restic.Snapshots {
	// Sort the snapshots so that the newer ones are listed first
	sort.SliceStable(list, func(i, j int) bool {
		return list[i].Time.After(list[j].Time)
	})

	var results restic.Snapshots
	seen := make(map[filterLastSnapshotsKey]int)
	for _, sn := range list {
		key := newFilterLastSnapshotsKey(sn)
		if seen[key] < limit {
			seen[key]++
			results = append(results, sn)
		}
	}
	return results
}

// calculateSnapshotsUsage calculates storage usage for snapshots.
// The oldest snapshot that references a blob is attributed the blob's size.
func calculateSnapshotsUsage(ctx context.Context, repo restic.Repository, snapshots restic.Snapshots, p *progress.Counter) (map[restic.ID]uint64, error) {
	sortedSnapshots := make([]*restic.Snapshot, len(snapshots))
	copy(sortedSnapshots, snapshots)
	sort.Slice(sortedSnapshots, func(i, j int) bool {
		return sortedSnapshots[i].Time.Before(sortedSnapshots[j].Time)
	})

	usage := make(map[restic.ID]uint64)
	seenBlobs := restic.NewBlobSet()
	recordBlobUsage := func(sn *restic.Snapshot, bh restic.BlobHandle) bool {
		if seenBlobs.Has(bh) {
			return true
		}
		seenBlobs.Insert(bh)

		if pbs := repo.LookupBlob(bh.Type, bh.ID); len(pbs) > 0 {
			// Length is the compressed and encrypted size of the blob
			usage[*sn.ID()] += uint64(pbs[0].Length)
		}

		return false
	}

	for _, sn := range sortedSnapshots {
		var lock sync.Mutex

		wg, ctx := errgroup.WithContext(ctx)
		treeStream := restic.StreamTrees(ctx, wg, repo, restic.IDs{*sn.Tree}, func(treeID restic.ID) bool {
			// locking is necessary the goroutine below concurrently adds data blobs
			lock.Lock()
			h := restic.BlobHandle{ID: treeID, Type: restic.TreeBlob}
			skip := recordBlobUsage(sn, h)
			lock.Unlock()
			return skip
		}, nil)

		wg.Go(func() error {
			for tree := range treeStream {
				if tree.Error != nil {
					return tree.Error
				}

				lock.Lock()
				for _, node := range tree.Nodes {
					switch node.Type {
					case restic.NodeTypeFile:
						for _, blob := range node.Content {
							h := restic.BlobHandle{ID: blob, Type: restic.DataBlob}
							recordBlobUsage(sn, h)
						}
					}
				}
				lock.Unlock()
			}
			return nil
		})
		if err := wg.Wait(); err != nil {
			return nil, err
		}

		if p != nil {
			p.Add(1)
		}
	}

	return usage, nil
}

// PrintSnapshots prints a text table of the snapshots in list to stdout.
func PrintSnapshots(stdout io.Writer, list restic.Snapshots, reasons []restic.KeepReason, usage map[restic.ID]uint64, compact bool) {
	// keep the reasons a snasphot is being kept in a map, so that it doesn't
	// get lost when the list of snapshots is sorted
	keepReasons := make(map[restic.ID]restic.KeepReason, len(reasons))
	if len(reasons) > 0 {
		for i, sn := range list {
			id := sn.ID()
			keepReasons[*id] = reasons[i]
		}
	}
	// check if any snapshot contains a summary
	hasSize := false
	for _, sn := range list {
		hasSize = hasSize || (sn.Summary != nil)
	}

	// check if we have usage data
	hasUsage := usage != nil

	// always sort the snapshots so that the newer ones are listed last
	sort.SliceStable(list, func(i, j int) bool {
		return list[i].Time.Before(list[j].Time)
	})

	// Determine the max widths for host and tag.
	maxHost, maxTag := 10, 6
	for _, sn := range list {
		if len(sn.Hostname) > maxHost {
			maxHost = len(sn.Hostname)
		}
		for _, tag := range sn.Tags {
			if len(tag) > maxTag {
				maxTag = len(tag)
			}
		}
	}

	tab := table.New()

	if compact {
		tab.AddColumn("ID", "{{ .ID }}")
		tab.AddColumn("Time", "{{ .Timestamp }}")
		tab.AddColumn("Host", "{{ .Hostname }}")
		tab.AddColumn("Tags  ", `{{ join .Tags "\n" }}`)
		if hasSize {
			tab.AddColumn("Size", `{{ .Size }}`)
		}
		if hasUsage {
			tab.AddColumn("Usage", `{{ .Usage }}`)
		}
	} else {
		tab.AddColumn("ID", "{{ .ID }}")
		tab.AddColumn("Time", "{{ .Timestamp }}")
		tab.AddColumn("Host      ", "{{ .Hostname }}")
		tab.AddColumn("Tags      ", `{{ join .Tags "," }}`)
		if len(reasons) > 0 {
			tab.AddColumn("Reasons", `{{ join .Reasons "\n" }}`)
		}
		tab.AddColumn("Paths", `{{ join .Paths "\n" }}`)
		if hasSize {
			tab.AddColumn("Size", `{{ .Size }}`)
		}
		if hasUsage {
			tab.AddColumn("Usage", `{{ .Usage }}`)
		}
	}

	type snapshot struct {
		ID        string
		Timestamp string
		Hostname  string
		Tags      []string
		Reasons   []string
		Paths     []string
		Size      string
		Usage     string
	}

	var multiline bool
	for _, sn := range list {
		data := snapshot{
			ID:        sn.ID().Str(),
			Timestamp: sn.Time.Local().Format(TimeFormat),
			Hostname:  sn.Hostname,
			Tags:      sn.Tags,
			Paths:     sn.Paths,
		}

		if len(reasons) > 0 {
			id := sn.ID()
			data.Reasons = keepReasons[*id].Matches
		}

		if len(sn.Paths) > 1 && !compact {
			multiline = true
		}

		if sn.Summary != nil {
			data.Size = ui.FormatBytes(sn.Summary.TotalBytesProcessed)
		}

		if hasUsage {
			id := sn.ID()
			usage := usage[*id]
			data.Usage = ui.FormatBytes(usage)
		}

		tab.AddRow(data)
	}

	tab.AddFooter(fmt.Sprintf("%d snapshots", len(list)))

	if multiline {
		// print an additional blank line between snapshots

		var last int
		tab.PrintData = func(w io.Writer, idx int, s string) error {
			var err error
			if idx == last {
				_, err = fmt.Fprintf(w, "%s\n", s)
			} else {
				_, err = fmt.Fprintf(w, "\n%s\n", s)
			}
			last = idx
			return err
		}
	}

	err := tab.Write(stdout)
	if err != nil {
		Warnf("error printing: %v\n", err)
	}
}

// PrintSnapshotGroupHeader prints which group of the group-by option the
// following snapshots belong to.
// Prints nothing, if we did not group at all.
func PrintSnapshotGroupHeader(stdout io.Writer, groupKeyJSON string) error {
	var key restic.SnapshotGroupKey

	err := json.Unmarshal([]byte(groupKeyJSON), &key)
	if err != nil {
		return err
	}

	if key.Hostname == "" && key.Tags == nil && key.Paths == nil {
		return nil
	}

	// Info
	if _, err := fmt.Fprintf(stdout, "snapshots"); err != nil {
		return err
	}
	var infoStrings []string
	if key.Hostname != "" {
		infoStrings = append(infoStrings, "host ["+key.Hostname+"]")
	}
	if key.Tags != nil {
		infoStrings = append(infoStrings, "tags ["+strings.Join(key.Tags, ", ")+"]")
	}
	if key.Paths != nil {
		infoStrings = append(infoStrings, "paths ["+strings.Join(key.Paths, ", ")+"]")
	}
	if infoStrings != nil {
		if _, err := fmt.Fprintf(stdout, " for (%s)", strings.Join(infoStrings, ", ")); err != nil {
			return err
		}
	}
	_, err = fmt.Fprintf(stdout, ":\n")

	return err
}

// Snapshot helps to print Snapshots as JSON with their ID included.
type Snapshot struct {
	*restic.Snapshot

	ID      *restic.ID `json:"id"`
	ShortID string     `json:"short_id"` // deprecated
}

// SnapshotGroup helps to print SnapshotGroups as JSON with their GroupReasons included.
type SnapshotGroup struct {
	GroupKey  restic.SnapshotGroupKey `json:"group_key"`
	Snapshots []Snapshot              `json:"snapshots"`
}

// printSnapshotGroupJSON writes the JSON representation of list to stdout.
func printSnapshotGroupJSON(stdout io.Writer, snGroups map[string]restic.Snapshots, grouped bool) error {
	if grouped {
		snapshotGroups := []SnapshotGroup{}

		for k, list := range snGroups {
			var key restic.SnapshotGroupKey
			var err error
			var snapshots []Snapshot

			err = json.Unmarshal([]byte(k), &key)
			if err != nil {
				return err
			}

			for _, sn := range list {
				k := Snapshot{
					Snapshot: sn,
					ID:       sn.ID(),
					ShortID:  sn.ID().Str(),
				}
				snapshots = append(snapshots, k)
			}

			group := SnapshotGroup{
				GroupKey:  key,
				Snapshots: snapshots,
			}
			snapshotGroups = append(snapshotGroups, group)
		}

		return json.NewEncoder(stdout).Encode(snapshotGroups)
	}

	// Old behavior
	snapshots := []Snapshot{}

	for _, list := range snGroups {
		for _, sn := range list {
			k := Snapshot{
				Snapshot: sn,
				ID:       sn.ID(),
				ShortID:  sn.ID().Str(),
			}
			snapshots = append(snapshots, k)
		}
	}

	return json.NewEncoder(stdout).Encode(snapshots)
}
