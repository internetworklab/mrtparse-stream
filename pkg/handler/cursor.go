package handler

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Cursor[T any] struct {
	cursorId     string
	lifeSpan     time.Duration
	ProducerChan chan T
	CancelFunc   context.CancelFunc
}

func NewCursor[T any](lifeSpan time.Duration, canceller context.CancelFunc) *Cursor[T] {
	return &Cursor[T]{
		cursorId:     uuid.NewString(),
		lifeSpan:     lifeSpan,
		ProducerChan: make(chan T),
		CancelFunc:   canceller,
	}
}

func (cursorObj *Cursor[T]) GetId() string {
	return cursorObj.cursorId
}

// Run() blocks until the end of the cursor's lifecycle: either expired or upstream closed.
func (cursorObj *Cursor[T]) Run(upstreamChan <-chan T) {
	cLife := cursorObj.lifeSpan
	defer close(cursorObj.ProducerChan)
	for {
		select {
		case <-time.After(cLife):
			return
		case val, ok := <-upstreamChan:
			if !ok {
				// upstream is closed, so the cursor have to be close as well.
				return
			}
			cursorObj.ProducerChan <- val
			// activity took place, so, refresh the expiry
			continue
		}
	}
}
