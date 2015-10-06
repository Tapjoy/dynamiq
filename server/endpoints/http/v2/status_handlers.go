package httpv2

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func (h *HTTPApi) statusServers(w http.ResponseWriter, r *http.Request) {
	response := make([]string, 0)
	for _, member := range h.memberList.Members() {
		response = append(response, fmt.Sprintf("Member: %s %s", member.Name, member.Addr))
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (h *HTTPApi) statusPartitionRange(w http.ResponseWriter, r *http.Request) {

}
