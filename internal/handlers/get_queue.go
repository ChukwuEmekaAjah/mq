package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/ChukwuEmekaAjah/mq/internal/util"
	"github.com/ChukwuEmekaAjah/mq/mq"
)

// GetQueue retrieves a queue from record
func GetQueue(store *mq.Store, w http.ResponseWriter, req *http.Request) {
	queueName := req.PathValue("queueName")

	queue, err := store.GetQueue(queueName)
	if err != nil {
		response, marshalErr := json.Marshal(util.ResponseBody{
			Message: err.Error(),
			Data:    nil,
		})

		if marshalErr != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"message": "Queue could not be retrieved"}`))
			return
		}

		w.WriteHeader(http.StatusBadRequest)
		w.Write(response)
		return
	}

	w.WriteHeader(http.StatusOK)
	response, marshalErr := json.Marshal(util.ResponseBody{
		Message: "Successfully retrieved queue",
		Data: struct {
			ID         string
			QueueName  string
			Attributes mq.QueueAttributes
			Tags       map[string]string
		}{
			ID:         queue.ID,
			QueueName:  queue.QueueName,
			Attributes: queue.Attributes,
			Tags:       queue.Tags,
		}})

	if marshalErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(response)
}
