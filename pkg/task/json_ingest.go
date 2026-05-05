package task

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	pkgmodel "github.com/internetworklab/mrtparse-stream/pkg/model"
)

// JSONIngestTask reads MRT data from a source stream and prints each entry as
// a JSON object to the configured output, one per line.
type JSONIngestTask struct {
	source io.Reader
	output io.Writer
	limit  int
}

// JSONIngestTaskConfigurer is a function that configures a JSONIngestTask.
type JSONIngestTaskConfigurer func(*JSONIngestTask) *JSONIngestTask

// WithOutput returns a configurer that overrides the output writer.
// Defaults to os.Stdout if not specified.
func WithOutput(w io.Writer) JSONIngestTaskConfigurer {
	return func(t *JSONIngestTask) *JSONIngestTask {
		return &JSONIngestTask{
			source: t.source,
			output: w,
			limit:  t.limit,
		}
	}
}

// WithJSONLineIngestLimit returns a configurer that sets the maximum number of
// entries to process. A value of 0 means no limit.
func WithJSONLineIngestLimit(limit int) JSONIngestTaskConfigurer {
	return func(t *JSONIngestTask) *JSONIngestTask {
		return &JSONIngestTask{
			source: t.source,
			output: t.output,
			limit:  limit,
		}
	}
}

// NewJSONIngestTask creates a new JSONIngestTask.
func NewJSONIngestTask(source io.Reader, configurers ...JSONIngestTaskConfigurer) *JSONIngestTask {
	t := &JSONIngestTask{
		source: source,
		output: os.Stdout,
	}

	for _, c := range configurers {
		t = c(t)
	}

	return t
}

// Run executes the ingest pipeline: parse MRT entries from source and write
// their JSON representation to the configured output.
func (t *JSONIngestTask) Run(ctx context.Context) error {
	parser := pkgmodel.NewMRTParser(t.source)
	parser.Run(ctx)

	encoder := json.NewEncoder(t.output)
	var count int
	for {
		entry, err := parser.ReadEntry(ctx)
		if err != nil {
			break
		}

		if err := encoder.Encode(entry); err != nil {
			return fmt.Errorf("JSON encode failed: %w", err)
		}

		count++
		if t.limit > 0 && count >= t.limit {
			break
		}
	}

	return nil
}
