package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ChukwuEmekaAjah/mq/internal/util"
	"github.com/ChukwuEmekaAjah/mq/mq"
)

// DeleteMessage deletes a queue from record
func DeleteMessage(store *mq.Store, w http.ResponseWriter, req *http.Request) {
	_, err := store.DeleteMessage(req.PathValue("queueName"), req.PathValue("receiptHandleId"))
	if err != nil {
		response, marshalErr := json.Marshal(util.ResponseBody{
			Message: err.Error(),
			Data:    nil,
		})

		if marshalErr != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(fmt.Sprintf(`{"message": "%s"}`, err.Error())))
			return
		}

		w.WriteHeader(http.StatusBadRequest)
		w.Write(response)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	response, marshalErr := json.Marshal(util.ResponseBody{
		Message: "Successfully deleted message from queue",
		Data:    nil,
	})

	if marshalErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(response)
}
