package utils

import (
	"compress/bzip2"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

func isGzippedPath(path string) bool {
	return strings.HasSuffix(strings.ToLower(path), ".gz")
}

func isBzip2Path(path string) bool {
	return strings.HasSuffix(strings.ToLower(path), ".bz2")
}

type WrappedReadCloser struct {
	io.Reader
	closeFunc func() error
}

func (m *WrappedReadCloser) Close() error {
	return m.closeFunc()
}

func tryDecompress(raw io.ReadCloser, path string) (io.ReadCloser, error) {
	if isGzippedPath(path) {
		gr, err := gzip.NewReader(raw)
		if err != nil {
			raw.Close()
			return nil, fmt.Errorf("gzip.NewReader failed: %w", err)
		}
		return gr, nil
	}

	if isBzip2Path(path) {
		rdCloser := &WrappedReadCloser{
			Reader: bzip2.NewReader(raw),
			closeFunc: func() error {
				return raw.Close()
			},
		}
		return rdCloser, nil
	}

	return raw, nil
}

func GetDecompressedSourceReadCloser(ctx context.Context, urlOrPath string) (io.ReadCloser, error) {
	if strings.HasPrefix(urlOrPath, "http://") || strings.HasPrefix(urlOrPath, "https://") {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlOrPath, nil)
		if err != nil {
			return nil, fmt.Errorf("build request failed: %w", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("HTTP GET failed: %w", err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
		}

		return tryDecompress(resp.Body, urlOrPath)
	} else {
		f, err := os.Open(urlOrPath)
		if err != nil {
			return nil, fmt.Errorf("open file failed: %w", err)
		}

		return tryDecompress(f, urlOrPath)
	}
}
