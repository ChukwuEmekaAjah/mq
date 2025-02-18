package handlers

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/ChukwuEmekaAjah/mq/internal/util"
	"github.com/ChukwuEmekaAjah/mq/mq"
)

// TagQueue sets tags on a queue
func TagQueue(store *mq.Store, w http.ResponseWriter, req *http.Request) {
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
	var requestBody struct {
		Tags map[string]string
	}
	err = json.Unmarshal(requestBodyBytes, &requestBody)

	if err != nil {
		response, marshalErr := json.Marshal(util.ResponseBody{
			Message: "Invalid request body passed in. Should be a dict with `name`",
			Data:    nil,
		})

		if marshalErr != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"message": "Invalid request body"}`))
			return
		}

		w.WriteHeader(http.StatusBadRequest)
		w.Write(response)
		return
	}

	set, err := store.UpdateQueue(req.PathValue("queueName"), requestBody.Tags)

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
		Message: "Successfully set queue attributes",
		Data:    set,
	})

	if marshalErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(response)
}
