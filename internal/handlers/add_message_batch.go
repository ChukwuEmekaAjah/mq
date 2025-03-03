package handlers

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/ChukwuEmekaAjah/mq/internal/models"
	"github.com/ChukwuEmekaAjah/mq/internal/util"
	"github.com/ChukwuEmekaAjah/mq/mq"
)

// AddMessageBatch adds a set of messages up to 10 into the queue
func AddMessageBatch(store *mq.Store, w http.ResponseWriter, req *http.Request) {

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
		Entries []models.MessageRequest
	}

	err = json.Unmarshal(requestBodyBytes, &requestBody)
	if err != nil {
		response, _ := json.Marshal(util.ResponseBody{
			Message: "Invalid request body passed in. Should be an array of message requests",
			Data:    nil,
		})

		w.WriteHeader(http.StatusBadRequest)
		w.Write(response)
		return
	}

	result := store.AddMessageBatch(req.PathValue("queueName"), requestBody.Entries)

	w.WriteHeader(http.StatusOK)
	response, marshalErr := json.Marshal(util.ResponseBody{
		Message: "Successfully added messages to queue",
		Data:    result,
	})

	if marshalErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(response)
}
