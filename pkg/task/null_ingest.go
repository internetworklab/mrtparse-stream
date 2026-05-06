package task

import (
	"context"
	"fmt"
	"io"
	"time"

	pkgdb "github.com/internetworklab/mrtparse-stream/pkg/db"
	pkgmodel "github.com/internetworklab/mrtparse-stream/pkg/model"
)

// NullIngestTask reads MRT data from a source stream and discards every entry.
// It is useful for benchmarking the parser pipeline without a database backend.
type NullIngestTask struct {
	source       io.Reader
	showProgress bool
	showRate     bool
	limit        int
	samples      []rateSample // at most 2 elements: previous and current
}

// NullIngestTaskConfigurer is a function that configures a NullIngestTask.
type NullIngestTaskConfigurer func(*NullIngestTask) *NullIngestTask

// WithNullShowProgress returns a configurer that enables or disables progress output.
func WithNullShowProgress(show bool) NullIngestTaskConfigurer {
	return func(t *NullIngestTask) *NullIngestTask {
		return &NullIngestTask{
			source:       t.source,
			showProgress: show,
			showRate:     t.showRate,
			limit:        t.limit,
		}
	}
}

// WithNullShowRate returns a configurer that enables processing rate output
// (rows/sec) alongside progress. Has no effect unless progress output is also
// enabled via WithNullShowProgress.
func WithNullShowRate(show bool) NullIngestTaskConfigurer {
	return func(t *NullIngestTask) *NullIngestTask {
		return &NullIngestTask{
			source:       t.source,
			showProgress: t.showProgress,
			showRate:     show,
			limit:        t.limit,
		}
	}
}

// WithNullIngestLimit returns a configurer that sets the maximum number of
// entries to process. A value of 0 means no limit.
func WithNullIngestLimit(limit int) NullIngestTaskConfigurer {
	return func(t *NullIngestTask) *NullIngestTask {
		return &NullIngestTask{
			source:       t.source,
			showProgress: t.showProgress,
			showRate:     t.showRate,
			limit:        limit,
		}
	}
}

// NewNullIngestTask creates a new NullIngestTask.
func NewNullIngestTask(source io.Reader, configurers ...NullIngestTaskConfigurer) *NullIngestTask {
	t := &NullIngestTask{
		source: source,
	}

	for _, c := range configurers {
		t = c(t)
	}

	return t
}

// Run executes the null ingest pipeline: parse MRT entries from source and
// discard them via a NullMRTEntriesWriteCloser.
func (t *NullIngestTask) Run(ctx context.Context) error {
	writer := pkgdb.NewNullMRTEntriesWriteCloser()
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

		if err := writer.WriteMRTEntry(ctx, entry); err != nil {
			return fmt.Errorf("WriteMRTEntry failed at entry %d: %w", count+1, err)
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

	if err := writer.Close(); err != nil {
		return err
	}
	return nil
}

// recordSample shifts the window: the current sample becomes the previous,
// and the new sample takes the current slot.
func (t *NullIngestTask) recordSample(count int) {
	t.samples[0] = t.samples[1]
	t.samples[1] = rateSample{count: count, ts: time.Now()}
}

// printProgress prints the current processing count. If showRate is enabled
// and two samples are available, it prints the instant rate between the
// two most recent samples in rows/sec.
func (t *NullIngestTask) printProgress(ctx context.Context, count int) {
	logger := getLogger(ctx)
	if t.showRate {
		deltaCount := t.samples[1].count - t.samples[0].count
		deltaSec := t.samples[1].ts.Sub(t.samples[0].ts).Seconds()
		if deltaSec > 0 {
			logger.Printf("%d parsed (%.0f rows/sec)", count, float64(deltaCount)/deltaSec)
			return
		}
	}
	logger.Printf("%d parsed", count)
}
