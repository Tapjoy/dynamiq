package httpv2

import (
	"encoding/json"
	"net/http"

	"github.com/Tapjoy/dynamiq/core"
	"github.com/gorilla/mux"
)

func (h *HTTPApi) queueList(w http.ResponseWriter, r *http.Request) {
	response(w, http.StatusOK, map[string]interface{}{"queues": h.context.QueueNames()})
}

func (h *HTTPApi) queueDetails(w http.ResponseWriter, r *http.Request) {
	queueName := mux.Vars(r)["queue"]
	conf, err := h.context.GetQueueConfig(queueName)
	if err != nil {
		errorResponse(w, err)
	}
	response(w, http.StatusOK, map[string]interface{}{"queue": queueName, "config": conf})
}

func (h *HTTPApi) queueConfigure(w http.ResponseWriter, r *http.Request) {

}

func (h *HTTPApi) queueCreate(w http.ResponseWriter, r *http.Request) {
	queueName := mux.Vars(r)["queue"]
	ok, err := h.context.Queues.Create(queueName, core.DefaultSettings)
	if err != nil {
		// return 500 for now - should return contextually correct errors otherwise
		errorResponse(w, err)
	}
	response(w, http.StatusOK, map[string]interface{}{"queue": queueName, "created": ok})

}

func (h *HTTPApi) queueDelete(w http.ResponseWriter, r *http.Request) {

}

func (h *HTTPApi) queueSubmitMessage(w http.ResponseWriter, r *http.Request) {
	queueName := mux.Vars(r)["queue"]
	msgData := "" // TODO read from body

	id, err := h.context.Queues.SaveMessage(queueName, msgData)
	if err != nil {
		errorResponse(w, err)
	}
	response(w, http.StatusOK, map[string]interface{}{"queue": queueName, "id": id})
}

func (h *HTTPApi) queueGetMessage(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	queueName := vars["queue"]
	id := vars["id"]
	msg, err := h.context.Queues.GetMessage(queueName, id)
	if err != nil {
		errorResponse(w, err)
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(msg)
}

func (h *HTTPApi) queuePollMessage(w http.ResponseWriter, r *http.Request) {

}

func (h *HTTPApi) queueDeleteMessage(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	queueName := vars["queue"]
	id := vars["id"]

	ok, err := h.context.Queues.DeleteMessage(queueName, id)
	if err != nil {
		errorResponse(w, err)
	}
	response(w, http.StatusOK, map[string]interface{}{"queue": queueName, "deleted": ok})

}
