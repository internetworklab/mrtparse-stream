package lister

import "context"

type MockProvidersLister struct{}

func (m *MockProvidersLister) List(_ context.Context) (any, error) {
	return []string{}, nil
}

func (m *MockProvidersLister) ListAsStream(ctx context.Context) (<-chan any, error) {
	result, err := m.List(ctx)
	if err != nil {
		return nil, err
	}
	providers := result.([]string)

	ch := make(chan any)
	go func() {
		defer close(ch)
		for _, provider := range providers {
			ch <- provider
		}
	}()
	return ch, nil
}
