package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/ChukwuEmekaAjah/mq/internal/util"
	"github.com/ChukwuEmekaAjah/mq/mq"
)

// MaxReadMessages specifies the max number of messages that can be read from the queue at a time
const MaxReadMessages int64 = 10

// ReadMessages removes some messages from the queue to be deleted
func ReadMessages(store *mq.Store, w http.ResponseWriter, req *http.Request) {
	limit := MaxReadMessages
	err := errors.New("Messages could not be read")
	if req.URL.Query().Has("limit") {
		limit, err = strconv.ParseInt(req.URL.Query().Get("limit"), 10, 64)

		if err != nil {
			response, marshalErr := json.Marshal(util.ResponseBody{
				Message: "Invalid limit for messages to be read",
				Data:    nil,
			})

			if marshalErr != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(fmt.Sprintf(`{"message": "Invalid limit for messages to be read"}`)))
				return
			}

			w.WriteHeader(http.StatusBadRequest)
			w.Write(response)
			return
		}

		if limit < 1 {
			limit = int64(MaxReadMessages)
		}
	}

	messages, err := store.ReadMessages(req.PathValue("queueName"), uint(limit))
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
		Message: "Successfully retrieved messages from queue",
		Data:    messages,
	})

	if marshalErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(response)
}
