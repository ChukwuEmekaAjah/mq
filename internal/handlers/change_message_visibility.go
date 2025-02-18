package handlers

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/ChukwuEmekaAjah/mq/internal/models"
	"github.com/ChukwuEmekaAjah/mq/internal/util"
	"github.com/ChukwuEmekaAjah/mq/mq"
)

// ChangeMessageVisibilityTimeout changes the message visibility timeout
func ChangeMessageVisibilityTimeout(store *mq.Store, w http.ResponseWriter, req *http.Request) {
	requestBodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		response, err := json.Marshal(util.ResponseBody{
			Message: "Request body could not be read",
			Data:    nil,
		})

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusBadRequest)
		w.Write(response)
		return
	}
	requestBody := &models.ChangeMessageVisibilityRequest{}
	err = json.Unmarshal(requestBodyBytes, requestBody)

	if err != nil {
		response, _ := json.Marshal(util.ResponseBody{
			Message: "Invalid request body passed in. Should be a dict `ReceiptHandle` and `VisibilityTimeout` keys",
			Data:    nil,
		})

		w.WriteHeader(http.StatusBadRequest)
		w.Write(response)
		return
	}

	if req.PathValue("receiptHandle") != requestBody.ReceiptHandle {
		response, _ := json.Marshal(util.ResponseBody{
			Message: "Invalid request. Receipthandle in url path does not match request body ReceiptHandle",
			Data:    nil,
		})

		w.WriteHeader(http.StatusBadRequest)
		w.Write(response)
		return
	}

	set, err := store.UpdateMessage(req.PathValue("queueName"), *requestBody)

	if err != nil {
		response, marshalErr := json.Marshal(util.ResponseBody{
			Message: err.Error(),
			Data:    nil,
		})

		if marshalErr != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusExpectationFailed)
		w.Write(response)
		return
	}

	w.WriteHeader(http.StatusCreated)
	response, marshalErr := json.Marshal(util.ResponseBody{
		Message: "Successfully changed message visibility timeout",
		Data:    set,
	})

	if marshalErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(response)
}
