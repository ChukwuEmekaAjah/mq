package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/ChukwuEmekaAjah/mq/internal/models"
	"github.com/ChukwuEmekaAjah/mq/internal/util"
	"github.com/ChukwuEmekaAjah/mq/mq"
)

// MaxMessageSize specifies the maximum size of a queue message
const MaxMessageSize int = 256e3

// AddMessage adds a new message to the associated queue
func AddMessage(store *mq.Store, w http.ResponseWriter, req *http.Request) {
	requestBodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		response, marshalErr := json.Marshal(util.ResponseBody{
			Message: "Request body could not be read",
			Data:    nil,
		})

		if marshalErr != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusBadRequest)
		w.Write(response)
		return
	}

	if len(requestBodyBytes) > MaxMessageSize {
		response, marshalErr := json.Marshal(util.ResponseBody{
			Message: fmt.Sprintf("Queue message greater than max message size of %v bytes", MaxMessageSize),
			Data:    nil,
		})

		if marshalErr != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusBadRequest)
		w.Write(response)
		return
	}

	requestBody := &models.MessageRequest{}
	err = json.Unmarshal(requestBodyBytes, requestBody)
	if err != nil {
		response, _ := json.Marshal(util.ResponseBody{
			Message: "Request body could not be parsed",
			Data:    nil,
		})

		w.WriteHeader(http.StatusBadRequest)
		w.Write(response)
		return
	}

	messageID, err := store.AddMessage(req.PathValue("queueName"), requestBody)

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

	w.WriteHeader(http.StatusCreated)
	response, marshalErr := json.Marshal(util.ResponseBody{
		Message: "Successfully added message to queue",
		Data:    messageID,
	})

	if marshalErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(response)
}
