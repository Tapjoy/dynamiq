package httpv2

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/Tapjoy/dynamiq/core"
	"github.com/gorilla/mux"
)

// HTTPApi represents the object used to govern http calls into the system
type HTTPApi struct {
	context *core.Config
}

// New initializes a new
func New(cfg *core.Config) (*HTTPApi, error) {
	h := &HTTPApi{
		context: cfg,
	}
	router := mux.NewRouter().PathPrefix("/v2").Subrouter()

	statusRoutes := router.PathPrefix("/status").Subrouter()
	topicRoutes := router.PathPrefix("/topics").Subrouter()
	queueRoutes := router.PathPrefix("/queues").Subrouter()

	statusRoutes.HandleFunc("/server", h.statusServers).Methods("GET")
	statusRoutes.HandleFunc("/partitionrange", h.statusPartitionRange).Methods("GET")

	topicRoutes.HandleFunc("/", h.topicList).Methods("GET")
	topicRoutes.HandleFunc("/{topic}", h.topicDetails).Methods("GET")
	topicRoutes.HandleFunc("/{topic}", h.topicCreate).Methods("PUT")
	topicRoutes.HandleFunc("/{topic}", h.topicDelete).Methods("DELETE")
	topicRoutes.HandleFunc("/{topic}", h.topicSubmitMessage).Methods("POST")
	topicRoutes.HandleFunc("/{topic}/queues/{queue}", h.topicSubscribe).Methods("PUT")
	topicRoutes.HandleFunc("/{topic}/queues/{queue}", h.topicUnsubscribe).Methods("DELETE")

	queueRoutes.HandleFunc("/", h.queueList).Methods("GET")
	queueRoutes.HandleFunc("/{queue}", h.queueDetails).Methods("GET")
	queueRoutes.HandleFunc("/{queue}", h.queueConfigure).Methods("PATCH")
	queueRoutes.HandleFunc("/{queue}", h.queueCreate).Methods("PUT")
	queueRoutes.HandleFunc("/{queue}", h.queueDelete).Methods("DELETE")
	queueRoutes.HandleFunc("/{queue}", h.queueSubmitMessage).Methods("POST")
	queueRoutes.HandleFunc("/{queue}/{id}", h.queueGetMessage).Methods("GET")
	queueRoutes.HandleFunc("/{queue}/{id}", h.queueDeleteMessage).Methods("DELETE")
	queueRoutes.HandleFunc("/{queue}/poll/{num}", h.queuePollMessage).Methods("GET")

	http.Handle("/", router)
	return h, nil
}

// Listen is
func (h *HTTPApi) Listen() {
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", h.context.HTTP.Port), nil))
}

func response(w http.ResponseWriter, responsePayload map[string]interface{}) {
	json.NewEncoder(w).Encode(responsePayload)
}

func errorResponse(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	json.NewEncoder(w).Encode(map[string]error{"error": err})
}
