package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	pkgdb "github.com/internetworklab/mrtparse-stream/pkg/db"
)

const defaultCursorLifespan = 30 * time.Minute

func NewMRTEntriesQueryHandler(querier pkgdb.MRTEntriesReader) *MRTEntriesQueryHandler {
	return &MRTEntriesQueryHandler{
		Querier:    querier,
		cursorsMap: sync.Map{},
	}
}

type MRTEntriesQueryHandler struct {
	Querier    pkgdb.MRTEntriesReader
	cursorsMap sync.Map
}

func (h *MRTEntriesQueryHandler) getProvider(r *http.Request) string {
	return r.PathValue("provider")
}

// the context should be producer context, must not be mixed up with http request context
func (h *MRTEntriesQueryHandler) getProducerOrRespondWithError(ctx context.Context, w http.ResponseWriter, r *http.Request) <-chan pkgdb.MRTEntryDataEvent {
	q := r.URL.Query()

	provider := h.getProvider(r)
	if provider == "" {
		writeErr(w, http.StatusBadRequest, "missing provider")
		return nil
	}

	if originASN := q.Get(QueryKeyOriginASN); originASN != "" {
		asn, parseErr := strconv.ParseUint(originASN, 10, 32)
		if parseErr != nil {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("invalid %s: %v", QueryKeyOriginASN, parseErr))
			return nil
		}
		return h.Querier.GetMRTEntriesByOriginAS(ctx, uint32(asn), provider)
	} else if asSegments := q.Get(QueryKeyASSegments); asSegments != "" {
		segments, parseErr := parseUint32List(asSegments)
		if parseErr != nil {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("invalid %s: %v", QueryKeyASSegments, parseErr))
			return nil
		}
		return h.Querier.GetMRTEntriesByASSegments(ctx, segments, provider)
	} else if neighborASN := q.Get(QueryKeyNeighborASN); neighborASN != "" {
		asn, parseErr := strconv.ParseUint(neighborASN, 10, 32)
		if parseErr != nil {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("invalid %s: %v", QueryKeyNeighborASN, parseErr))
			return nil
		}
		return h.Querier.GetMRTEntriesByNeighborAS(ctx, uint32(asn), provider)
	} else if ip := q.Get(QueryKeyIP); ip != "" {
		parsedIP := net.ParseIP(ip)
		if parsedIP == nil {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("invalid %s: %q is not a valid IP address", QueryKeyIP, ip))
			return nil
		}
		return h.Querier.GetMRTEntriesByIP(ctx, parsedIP, provider)
	} else if cidr := q.Get(QueryKeyCIDR); cidr != "" {
		_, parsedCIDR, parseErr := net.ParseCIDR(cidr)
		if parseErr != nil {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("invalid %s: %v", QueryKeyCIDR, parseErr))
			return nil
		}
		return h.Querier.GetMRTEntriesByCIDR(ctx, *parsedCIDR, provider)
	} else {
		writeErr(w, http.StatusBadRequest, "missing query parameter: one of originAsn, asSegments, neighborAsn, ip, or cidr is required")
		return nil
	}
}

func (h *MRTEntriesQueryHandler) getPS(r *http.Request) int {
	if psStr := r.URL.Query().Get(QueryKeyPageSize); psStr != "" {
		if x, err := strconv.ParseInt(psStr, 10, 0); err == nil && x > 0 {
			return int(x)
		}
	}
	return 0 // 0 means no pagination, just returns all data
}

func (h *MRTEntriesQueryHandler) getCursorLifeSpan(r *http.Request) time.Duration {
	lifeSpan := defaultCursorLifespan
	if s := r.URL.Query().Get(QueryKeyCursorLifespan); s != "" {
		if parsed, err := time.ParseDuration(s); err == nil {
			lifeSpan = parsed
		}
	}
	return lifeSpan
}

func (h *MRTEntriesQueryHandler) serveChan(w http.ResponseWriter, r *http.Request, valCh <-chan pkgdb.MRTEntryDataEvent, cursorId string, pageSize int) {
	w.Header().Set(HeaderKeyCursorId, cursorId)
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher, canFlush := w.(http.Flusher)
	ctx := r.Context()
	recordsServed := 0
	for {
		select {
		case <-ctx.Done():
			// Note: we still needs the x-*-cursor-id header, becuase the connection might be close by the client,
			// in such case we have no chance to set the cursor id on the data event.
			return
		case val, ok := <-valCh:
			if !ok {
				return
			}

			recordsServed++
			if pageSize > 0 && recordsServed >= pageSize {
				json.NewEncoder(w).Encode(ResumableResponseStreamEvent{
					Data:     val,
					CursorID: cursorId,
				})
				return
			}

			json.NewEncoder(w).Encode(ResumableResponseStreamEvent{
				Data: val,
			})
			if canFlush {
				flusher.Flush()
			}
		}
	}
}

func (h *MRTEntriesQueryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if cursorId := r.URL.Query().Get(QueryKeyCursorID); cursorId != "" {
		if cursorAny, hit := h.cursorsMap.Load(cursorId); hit {
			cursor, ok := cursorAny.(*Cursor[pkgdb.MRTEntryDataEvent])
			if !ok {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(ErrResp{Err: "wrong cursor map value type"})
				return
			}
			h.serveChan(w, r, cursor.ProducerChan, cursorId, h.getPS(r))
		} else {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(ErrResp{Err: fmt.Sprintf("cursor %s not found", cursorId)})
		}
		return
	}

	producerCtx := context.Background()
	producerCancellableCtx, producerCtxCancelFunc := context.WithCancel(producerCtx)
	producerCh := h.getProducerOrRespondWithError(producerCancellableCtx, w, r)
	if producerCh == nil {
		// if that function encountered an error, it will returns a nil
		producerCtxCancelFunc()
		return
	}

	lifeSpan := h.getCursorLifeSpan(r)
	cursorObj := NewCursor[pkgdb.MRTEntryDataEvent](lifeSpan, producerCtxCancelFunc)
	cId := cursorObj.GetId()
	h.cursorsMap.Store(cId, cursorObj)

	go func(cursorObj *Cursor[pkgdb.MRTEntryDataEvent]) {
		cursorObj.Run(producerCh) // <- blocks here until cursor dies
		h.cursorsMap.Delete(cId)
	}(cursorObj)

	h.serveChan(w, r, cursorObj.ProducerChan, cId, h.getPS(r))
}
