package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/ChukwuEmekaAjah/mq/internal/util"
	"github.com/ChukwuEmekaAjah/mq/mq"
)

// PurgeQueue deletes all the messages in a queue
func PurgeQueue(store *mq.Store, w http.ResponseWriter, req *http.Request) {
	queueName := req.PathValue("queueName")

	_, err := store.PurgeQueue(queueName)
	if err != nil {
		response, marshalErr := json.Marshal(util.ResponseBody{
			Message: err.Error(),
			Data:    nil,
		})

		if marshalErr != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"message": "Queue could not be purged"}`))
			return
		}

		w.WriteHeader(http.StatusBadRequest)
		w.Write(response)
		return
	}

	w.WriteHeader(http.StatusOK)
	response, marshalErr := json.Marshal(util.ResponseBody{
		Message: "Successfully purged queue",
		Data:    nil,
	})

	if marshalErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(response)
}
