package lister

import (
	"context"
	"fmt"

	"github.com/internetworklab/mrtparse-stream/pkg/db"
)

type DBProvidersLister struct {
	reader db.ProvidersReader
}

func NewDBProvidersLister(reader db.ProvidersReader) *DBProvidersLister {
	return &DBProvidersLister{reader: reader}
}

func (l *DBProvidersLister) List(ctx context.Context) (any, error) {
	entries, err := l.reader.GetProviders(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list providers: %w", err)
	}
	return entries, nil
}

func (l *DBProvidersLister) ListAsStream(ctx context.Context) (<-chan any, error) {
	entryCh, err := l.reader.GetProvidersAsStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to stream providers: %w", err)
	}

	ch := make(chan any)
	go func() {
		defer close(ch)
		for entry := range entryCh {
			select {
			case ch <- entry:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}
