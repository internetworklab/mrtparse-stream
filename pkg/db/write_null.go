package db

import (
	"context"

	pkgmodel "github.com/internetworklab/mrtparse-stream/pkg/model"
)

// Compile-time check that NullMRTEntriesWriteCloser implements MRTEntriesWriteCloser.
var _ MRTEntriesWriteCloser = (*NullMRTEntriesWriteCloser)(nil)

// NullMRTEntriesWriteCloser is a no-op MRTEntriesWriteCloser that silently discards every entry.
type NullMRTEntriesWriteCloser struct{}

func NewNullMRTEntriesWriteCloser() *NullMRTEntriesWriteCloser {
	return &NullMRTEntriesWriteCloser{}
}

func (n *NullMRTEntriesWriteCloser) WriteMRTEntry(_ context.Context, _ *pkgmodel.MRTEntry) error {
	return nil
}

func (n *NullMRTEntriesWriteCloser) Close() error {
	return nil
}
