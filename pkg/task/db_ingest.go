package task

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	pkgdb "github.com/internetworklab/mrtparse-stream/pkg/db"
	pkgmodel "github.com/internetworklab/mrtparse-stream/pkg/model"
	pkgutils "github.com/internetworklab/mrtparse-stream/pkg/utils"
)

// rateSample holds a single (count, time) sample point for rate calculation.
type rateSample struct {
	count int
	ts    time.Time
}

// DBIngestTask reads MRT data from a source stream and ingests it into the
// database via the provided MRTEntriesWriteCloser.
type DBIngestTask struct {
	source            io.Reader
	writer            pkgdb.MRTEntriesWriteCloser
	showProgress      bool
	showRate          bool
	exitOnInsertError bool
	limit             int
	samples           []rateSample // at most 2 elements: previous and current
}

// DBIngestTaskConfigurer is a function that configures an IngestTask.
type DBIngestTaskConfigurer func(*DBIngestTask) *DBIngestTask

// getLogger extracts a *log.Logger from the given context. If no logger is
// found, it falls back to the standard logger with log.LstdFlags.
func getLogger(ctx context.Context) *log.Logger {
	if l, ok := ctx.Value(pkgutils.CtxKeyLogger).(*log.Logger); ok {
		return l
	}
	return log.Default()
}

// WithShowProgress returns a configurer that enables or disables progress output.
func WithShowProgress(showProgress bool) DBIngestTaskConfigurer {
	return func(t *DBIngestTask) *DBIngestTask {
		return &DBIngestTask{
			source:            t.source,
			writer:            t.writer,
			showProgress:      showProgress,
			showRate:          t.showRate,
			exitOnInsertError: t.exitOnInsertError,
			limit:             t.limit,
		}
	}
}

// WithShowRate returns a configurer that enables ingestion rate output (rows/sec)
// alongside progress. Has no effect unless progress output is also enabled via
// WithShowProgress.
func WithShowRate(showRate bool) DBIngestTaskConfigurer {
	return func(t *DBIngestTask) *DBIngestTask {
		return &DBIngestTask{
			source:            t.source,
			writer:            t.writer,
			showProgress:      t.showProgress,
			showRate:          showRate,
			exitOnInsertError: t.exitOnInsertError,
			limit:             t.limit,
		}
	}
}

// WithPGIngestLimit returns a configurer that sets the maximum number of
// entries to ingest. A value of 0 means no limit.
func WithPGIngestLimit(limit int) DBIngestTaskConfigurer {
	return func(t *DBIngestTask) *DBIngestTask {
		return &DBIngestTask{
			source:            t.source,
			writer:            t.writer,
			showProgress:      t.showProgress,
			showRate:          t.showRate,
			exitOnInsertError: t.exitOnInsertError,
			limit:             limit,
		}
	}
}

// WithExitOnInsertError returns a configurer that controls whether the task
// aborts on a failed insertion. When set to true, the task returns immediately
// on the first insertion error. When false (default), the error is logged and
// ingestion continues.
func WithExitOnInsertError(exit bool) DBIngestTaskConfigurer {
	return func(t *DBIngestTask) *DBIngestTask {
		return &DBIngestTask{
			source:            t.source,
			writer:            t.writer,
			showProgress:      t.showProgress,
			showRate:          t.showRate,
			exitOnInsertError: exit,
			limit:             t.limit,
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
			getLogger(ctx).Printf("ReadEntry finished at count %d: %v", count, err)
			break
		}

		if err := t.writer.WriteMRTEntry(ctx, entry); err != nil {
			if t.exitOnInsertError {
				return fmt.Errorf("WriteMRTEntry failed at entry %d: %w", count+1, err)
			}
			getLogger(ctx).Printf("WriteMRTEntry failed at entry %d: %v", count+1, err)
		}

		count++
		if t.showProgress && count%100 == 0 {
			t.recordSample(count)
			t.printProgress(ctx, count)
		}

		if t.limit > 0 && count >= t.limit {
			break
		}
	}
	if t.showProgress {
		t.recordSample(count)
		t.printProgress(ctx, count)
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
func (t *DBIngestTask) printProgress(ctx context.Context, count int) {
	logger := getLogger(ctx)
	if t.showRate {
		deltaCount := t.samples[1].count - t.samples[0].count
		deltaSec := t.samples[1].ts.Sub(t.samples[0].ts).Seconds()
		if deltaSec > 0 {
			logger.Printf("%d ingested (%.0f rows/sec)", count, float64(deltaCount)/deltaSec)
			return
		}
	}
	logger.Printf("%d ingested", count)
}
