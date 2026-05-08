package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

// this is a resumable / suspendable counter poc/demo.
type CounterHandler struct {
	cursorsMap sync.Map
}

func NewCounterHandler() *CounterHandler {
	return &CounterHandler{
		cursorsMap: sync.Map{},
	}
}

func (h *CounterHandler) newCounter(ctx context.Context, maxVal int, tickIntv time.Duration) <-chan int {
	valsCh := make(chan int)
	ticker := time.NewTicker(tickIntv)
	counterId := uuid.NewString()
	go func() {
		defer log.Printf("counter %s is closed", counterId)
		defer close(valsCh)
		defer ticker.Stop()

		log.Printf("counter %s is started", counterId)
		val := 0

		for {
			select {
			case <-ctx.Done():
				log.Printf("counter %s: ctx canceled", counterId)
				return
			case t, ok := <-ticker.C:
				if !ok {
					log.Printf("counter %s: ticker is closed", counterId)
					return
				}
				log.Printf("counter %s: ticks at %s, val=%d", counterId, t.Format(time.RFC3339Nano), val)
				valsCh <- val
				val++
				if val >= maxVal {
					log.Printf("counter %s: maxVal %d reached", counterId, maxVal)
					return
				}
			}
		}
	}()
	return valsCh
}

func (h *CounterHandler) serveChan(w http.ResponseWriter, r *http.Request, valCh <-chan int, cursorId string, pageSize int) {
	w.Header().Set(HeaderKeyCursorId, cursorId)
	w.Header().Set("content-type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	flusher, canFlush := w.(http.Flusher)
	ctx := r.Context()
	recordsServed := 0
	for {
		select {
		case <-ctx.Done():
			// Note: we still needs the x-*-cursor-id header, becuase the connection might be close by the client,
			// in such case we have no chance to set the cursor id on the data event.
			log.Printf("cursor id %s, remote: %s connection closed.", cursorId, r.RemoteAddr)
			return
		case val, ok := <-valCh:
			if !ok {
				log.Printf("cursor id %s, remote: %s, valCh closed.", cursorId, r.RemoteAddr)
				return
			}

			recordsServed++
			if pageSize > 0 && recordsServed >= pageSize {
				log.Printf("cursor id %s, remote: %s, served %d records, reached page size limit", cursorId, r.RemoteAddr, recordsServed)
				json.NewEncoder(w).Encode(ResumableResponseStreamEvent{
					Data:     val,
					CursorID: cursorId,
				})
				return
			}

			json.NewEncoder(w).Encode(ResumableResponseStreamEvent{
				Data: val,
			})
			log.Printf("cursor id %s, remote: %s, sent value: %d", cursorId, r.RemoteAddr, val)
			if canFlush {
				flusher.Flush()
			}
		}
	}
}

func (h *CounterHandler) getPS(r *http.Request) int {
	if psStr := r.URL.Query().Get(QueryKeyPageSize); psStr != "" {
		if x, err := strconv.ParseInt(psStr, 10, 0); err == nil && x > 0 {
			return int(x)
		}
	}
	return 0 // 0 means no pagination, just returns all data
}

func (h *CounterHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if cursorId := r.URL.Query().Get(QueryKeyCursorID); cursorId != "" {
		if cursorAny, hit := h.cursorsMap.Load(cursorId); hit {
			cursor, ok := cursorAny.(*Cursor[int])
			if !ok {
				log.Panicf("wrong cursor map value type")
				return
			}
			h.serveChan(w, r, cursor.ProducerChan, cursorId, h.getPS(r))
		} else {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "cursor %s not found", cursorId)
		}
		return
	}

	maxVal, err := strconv.ParseInt(r.URL.Query().Get(QueryKeyCounterMaxVal), 10, 0)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid max value: %v", err)
		return
	}

	tickIntv, err := time.ParseDuration(r.URL.Query().Get(QueryKeyCounterTickIntv))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid tick interval value: %v", err)
		return
	}

	lifeSpan, err := time.ParseDuration(r.URL.Query().Get(QueryKeyCursorLifespan))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid cursor lifespan value: %v", err)
		return
	}

	maxValInt := int(maxVal)

	// NOTE: DO NOT mix up request context with counter context !!
	counterCtx := context.Background()
	counterCancallableCtx, counterCancelFunc := context.WithCancel(counterCtx)

	valCh := h.newCounter(counterCancallableCtx, maxValInt, tickIntv)

	cursorObj := NewCursor[int](
		lifeSpan,
		counterCancelFunc,
	)

	cId := cursorObj.GetId()
	h.cursorsMap.Store(cId, cursorObj)

	go func(cursorObj *Cursor[int]) {
		cursorObj.Run(valCh) // <- blocks here until cursor dies
		log.Printf("cursor %s timer expired, clearning up", cId)
		h.cursorsMap.Delete(cId)
		log.Printf("cursor %s is deleted", cId)
	}(cursorObj)

	log.Printf("cursor %s is created, maxVal=%d, tickIntv=%s, lifeSpan=%s, remote %s", cId, maxValInt, tickIntv.String(), lifeSpan.String(), r.RemoteAddr)
	h.serveChan(w, r, cursorObj.ProducerChan, cId, h.getPS(r))
}
