package handler

import (
	"encoding/json"
	"fmt"
	"net/http"

	lister "github.com/internetworklab/mrtparse-stream/pkg/lister"
)

type ProvidersQueryHandler struct {
	ProvidersLister lister.Lister
}

func (h *ProvidersQueryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	data, err := h.ProvidersLister.List(ctx)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrResp{Err: fmt.Sprintf("failed to list providers: %v", err)})
		return
	}

	json.NewEncoder(w).Encode(DataResp{Data: data})
}
