package httpv2

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

func (h *HTTPApi) topicList(w http.ResponseWriter, r *http.Request) {
	response(w, http.StatusOK, map[string]interface{}{"topics": h.context.TopicNames()})
}

func (h *HTTPApi) topicDetails(w http.ResponseWriter, r *http.Request) {
	topicName := mux.Vars(r)["topic"]
	t, err := h.context.GetTopic(topicName)
	if err != nil {
		errorResponse(w, err)
	}
	response(w, http.StatusOK, map[string]interface{}{"topic": topicName, "queues": t.GetQueueNames()})
}

func (h *HTTPApi) topicCreate(w http.ResponseWriter, r *http.Request) {
	topicName := mux.Vars(r)["topic"]
	res, err := h.context.Topics.Create(topicName)
	if err != nil {
		// return 500 for now - should return contextually correct errors otherwise
		errorResponse(w, err)
	}
	response(w, http.StatusOK, map[string]interface{}{"topic": topicName, "created": res})
}

func (h *HTTPApi) topicDelete(w http.ResponseWriter, r *http.Request) {

}

func (h *HTTPApi) topicSubmitMessage(w http.ResponseWriter, r *http.Request) {
	topicName := mux.Vars(r)["topic"]
	msgData := "" // TODO read from body

	results := h.context.Topics.BroadcastMessage(topicName, msgData)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(results)
}

func (h *HTTPApi) topicSubscribe(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	topicName := vars["topic"]
	queueName := vars["queue"]

	ok, err := h.context.Topics.SubscribeQueue(topicName, queueName)
	resp := map[string]interface{}{"topic": topicName, "queue": queueName, "subscribed": ok}
	if err != nil {
		resp["error"] = err.Error()
	}
	response(w, http.StatusOK, resp)
}

func (h *HTTPApi) topicUnsubscribe(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	topicName := vars["topic"]
	queueName := vars["queue"]

	ok, err := h.context.Topics.UnsubscribeQueue(topicName, queueName)
	resp := map[string]interface{}{"topic": topicName, "queue": queueName, "unsubscribed": ok}
	if err != nil {
		resp["error"] = err.Error()
	}
	response(w, http.StatusOK, resp)
}
