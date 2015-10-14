package httpv2

import (
	"net/http"

	"github.com/gorilla/mux"
)

func (h *HTTPApi) topicList(w http.ResponseWriter, r *http.Request) {
	// TODO need to move topics/queues into a config manager to lock their access
	// due to the goroutines that update and need to lock them
}

func (h *HTTPApi) topicDetails(w http.ResponseWriter, r *http.Request) {

}

func (h *HTTPApi) topicCreate(w http.ResponseWriter, r *http.Request) {
	topicName := mux.Vars(r)["topic"]
	res, err := h.context.Topics.Create(topicName)
	if err != nil {
		// return 500 for now - should return contextually correct errors otherwise
		errorResponse(w, err)
	}
	response(w, map[string]interface{}{"topic": topicName, "created": res})
}

func (h *HTTPApi) topicDelete(w http.ResponseWriter, r *http.Request) {

}

func (h *HTTPApi) topicSubmitMessage(w http.ResponseWriter, r *http.Request) {

}

func (h *HTTPApi) topicSubscribe(w http.ResponseWriter, r *http.Request) {

}

func (h *HTTPApi) topicUnsubscribe(w http.ResponseWriter, r *http.Request) {

}
