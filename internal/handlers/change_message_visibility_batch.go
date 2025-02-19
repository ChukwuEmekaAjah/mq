package handlers

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/ChukwuEmekaAjah/mq/internal/models"
	"github.com/ChukwuEmekaAjah/mq/internal/util"
	"github.com/ChukwuEmekaAjah/mq/mq"
)

// DeleteMessageBatch deletes a queue from record
func DeleteMessageBatch(store *mq.Store, w http.ResponseWriter, req *http.Request) {

	requestBodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		response, _ := json.Marshal(util.ResponseBody{
			Message: "Request body could not be read",
			Data:    nil,
		})

		w.WriteHeader(http.StatusBadRequest)
		w.Write(response)
		return
	}

	var requestBody struct {
		Entries []models.ChangeMessageVisibilityRequest
	}
	err = json.Unmarshal(requestBodyBytes, &requestBody)
	if err != nil {
		response, _ := json.Marshal(util.ResponseBody{
			Message: "Invalid request body passed in. Should be a dict with `name`",
			Data:    nil,
		})

		w.WriteHeader(http.StatusBadRequest)
		w.Write(response)
		return
	}

	result := store.UpdateMessageBatch(req.PathValue("queueName"), requestBody.Entries)

	w.WriteHeader(http.StatusOK)
	response, marshalErr := json.Marshal(util.ResponseBody{
		Message: "Successfully changed message visibility timeouts for messages",
		Data:    result,
	})

	if marshalErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(response)
}
