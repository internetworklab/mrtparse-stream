package lister

import (
	"context"

	pkgdb "github.com/internetworklab/mrtparse-stream/pkg/db"
)

type ChannelListerWrapper struct {
	ch <-chan pkgdb.MRTEntryDataEvent
}

func ChannelLister(ch <-chan pkgdb.MRTEntryDataEvent) *ChannelListerWrapper {
	return &ChannelListerWrapper{ch: ch}
}

func (w *ChannelListerWrapper) List(ctx context.Context) (any, error) {
	stream, err := w.ListAsStream(ctx)
	if err != nil {
		return nil, err
	}
	var entries []any
	for item := range stream {
		entries = append(entries, item)
	}
	return entries, nil
}

func (w *ChannelListerWrapper) ListAsStream(ctx context.Context) (<-chan any, error) {
	out := make(chan any)
	go func() {
		defer close(out)
		for {
			select {
			case event, ok := <-w.ch:
				if !ok {
					return
				}
				select {
				case out <- event:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}
