package task

import (
	"context"
	"fmt"
	"io"
	"time"

	pkgdb "github.com/internetworklab/mrtparse-stream/pkg/db"
	pkgmodel "github.com/internetworklab/mrtparse-stream/pkg/model"
)

// rateSample holds a single (count, time) sample point for rate calculation.
type rateSample struct {
	count int
	ts    time.Time
}

// DBIngestTask reads MRT data from a source stream and ingests it into the
// database via the provided MRTEntriesWriteCloser.
type DBIngestTask struct {
	source       io.Reader
	writer       pkgdb.MRTEntriesWriteCloser
	showProgress bool
	showRate     bool
	limit        int
	samples      []rateSample // at most 2 elements: previous and current
}

// DBIngestTaskConfigurer is a function that configures an IngestTask.
type DBIngestTaskConfigurer func(*DBIngestTask) *DBIngestTask

// WithShowProgress returns a configurer that enables or disables progress output.
func WithShowProgress(showProgress bool) DBIngestTaskConfigurer {
	return func(t *DBIngestTask) *DBIngestTask {
		return &DBIngestTask{
			source:       t.source,
			writer:       t.writer,
			showProgress: showProgress,
			showRate:     t.showRate,
			limit:        t.limit,
		}
	}
}

// WithShowRate returns a configurer that enables ingestion rate output (rows/sec)
// alongside progress. Has no effect unless progress output is also enabled via
// WithShowProgress.
func WithShowRate(showRate bool) DBIngestTaskConfigurer {
	return func(t *DBIngestTask) *DBIngestTask {
		return &DBIngestTask{
			source:       t.source,
			writer:       t.writer,
			showProgress: t.showProgress,
			showRate:     showRate,
			limit:        t.limit,
		}
	}
}

// WithPGIngestLimit returns a configurer that sets the maximum number of
// entries to ingest. A value of 0 means no limit.
func WithPGIngestLimit(limit int) DBIngestTaskConfigurer {
	return func(t *DBIngestTask) *DBIngestTask {
		return &DBIngestTask{
			source:       t.source,
			writer:       t.writer,
			showProgress: t.showProgress,
			showRate:     t.showRate,
			limit:        limit,
		}
	}
}

// NewIngestTask creates a new IngestTask.
func NewIngestTask(source io.Reader, writer pkgdb.MRTEntriesWriteCloser, configurers ...DBIngestTaskConfigurer) *DBIngestTask {
	t := &DBIngestTask{
		source: source,
		writer: writer,
	}

	for _, c := range configurers {
		t = c(t)
	}

	return t
}

// Run executes the ingest pipeline: parse MRT entries from source and write
// them via the provided writer.
func (t *DBIngestTask) Run(ctx context.Context) error {
	parser := pkgmodel.NewMRTParser(t.source)
	parser.Run(ctx)

	t.samples = make([]rateSample, 2)
	t.samples[1] = rateSample{count: 0, ts: time.Now()}

	var count int
	for {
		entry, err := parser.ReadEntry(ctx)
		if err != nil {
			fmt.Printf("ReadEntry finished at count %d: %v\n", count, err)
			break
		}

		if err := t.writer.WriteMRTEntry(ctx, entry); err != nil {
			return fmt.Errorf("WriteMRTEntry failed at entry %d: %w", count+1, err)
		}

		count++
		if t.showProgress && count%100 == 0 {
			t.recordSample(count)
			t.printProgress(count)
		}

		if t.limit > 0 && count >= t.limit {
			break
		}
	}
	if t.showProgress {
		t.recordSample(count)
		t.printProgress(count)
	}

	if err := t.writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}
	return nil
}

// recordSample shifts the window: the current sample becomes the previous,
// and the new sample takes the current slot.
func (t *DBIngestTask) recordSample(count int) {
	t.samples[0] = t.samples[1]
	t.samples[1] = rateSample{count: count, ts: time.Now()}
}

// printProgress prints the current ingestion count. If showRate is enabled
// and two samples are available, it prints the instant rate between the
// two most recent samples in rows/sec.
func (t *DBIngestTask) printProgress(count int) {
	if t.showRate {
		deltaCount := t.samples[1].count - t.samples[0].count
		deltaSec := t.samples[1].ts.Sub(t.samples[0].ts).Seconds()
		if deltaSec > 0 {
			fmt.Printf("%d ingested (%.0f rows/sec)\n", count, float64(deltaCount)/deltaSec)
			return
		}
	}
	fmt.Printf("%d ingested\n", count)
}
