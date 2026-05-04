package task

import (
	"context"
	"fmt"
	"io"

	pkgdb "github.com/internetworklab/mrtparse-stream/pkg/db"
	pkgmodel "github.com/internetworklab/mrtparse-stream/pkg/model"
)

// IngestTask reads MRT data from a source stream and ingests it into the
// database via the provided MRTEntriesWriteCloser.
type IngestTask struct {
	source       io.Reader
	writer       pkgdb.MRTEntriesWriteCloser
	showProgress bool
}

// NewIngestTaskConfigurer is a function that configures an IngestTask.
type NewIngestTaskConfigurer func(*IngestTask) *IngestTask

// WithShowProgress returns a configurer that enables or disables progress output.
func WithShowProgress(showProgress bool) NewIngestTaskConfigurer {
	return func(t *IngestTask) *IngestTask {
		return &IngestTask{
			source:       t.source,
			writer:       t.writer,
			showProgress: showProgress,
		}
	}
}

// NewIngestTask creates a new IngestTask.
func NewIngestTask(source io.Reader, writer pkgdb.MRTEntriesWriteCloser, configurers ...NewIngestTaskConfigurer) *IngestTask {
	t := &IngestTask{
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
func (t *IngestTask) Run(ctx context.Context) error {
	parser := pkgmodel.NewMRTParser(t.source)
	parser.Run(ctx)

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
			fmt.Printf("%d ingested\n", count)
		}
	}
	if t.showProgress {
		fmt.Printf("%d ingested\n", count)
	}

	if err := t.writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}
	return nil
}
