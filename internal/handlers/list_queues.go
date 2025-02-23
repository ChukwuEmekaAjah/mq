package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/ChukwuEmekaAjah/mq/internal/util"
	"github.com/ChukwuEmekaAjah/mq/mq"
)

// ListQueues retrieves a queue from record
func ListQueues(store *mq.Store, w http.ResponseWriter, req *http.Request) {

	queues := store.ListQueues()

	w.WriteHeader(http.StatusOK)
	response, marshalErr := json.Marshal(util.ResponseBody{
		Message: "Successfully retrieved list of queues",
		Data:    queues,
	})

	if marshalErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(response)
}
