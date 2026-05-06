package handler

import (
	"encoding/json"
	"net/http"

	"github.com/internetworklab/mrtparse-stream/pkg/lister"
	"github.com/internetworklab/mrtparse-stream/pkg/utils"
)

type ListerStreamingHandler struct {
	lister lister.Lister
}

func NewListerStreamingHandler(l lister.Lister) *ListerStreamingHandler {
	return &ListerStreamingHandler{lister: l}
}

func (h *ListerStreamingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	ch, err := h.lister.ListAsStream(ctx)
	if err != nil {
		utils.GetLogger(ctx).Printf("failed to start stream: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrResp{Err: err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)

	flusher, canFlush := w.(http.Flusher)
	enc := json.NewEncoder(w)

	for item := range ch {
		if err := enc.Encode(item); err != nil {
			utils.GetLogger(ctx).Printf("failed to encode item: %v", err)
			return
		}
		if canFlush {
			flusher.Flush()
		}
	}
}
