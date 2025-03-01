package handlers

import (
	"fmt"
	"net/http"
	"time"

	"github.com/ChukwuEmekaAjah/mq/mq"
)

// ManageMessages manages messages sent into the queue
func ManageMessages(store *mq.Store) func(http.ResponseWriter, *http.Request) {

	return func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()
		func(w http.ResponseWriter, req *http.Request) {
			w.Header().Add("Content-Type", "application/json")

			if req.Pattern == "/queues/{queueName}/messages" {
				if req.Method == "POST" {
					AddMessage(store, w, req)
					return
				}

				if req.Method == "GET" {
					ReadMessages(store, w, req)
					return
				}

				if req.Method == "DELETE" {
					PurgeQueue(store, w, req)
					return
				}

				if req.Method == "PUT" {
					DeleteMessageBatch(store, w, req)
					return
				}

			}

			if req.Pattern == "/queues/{queueName}/messages/{receiptHandle}" {
				if req.Method == "DELETE" {
					DeleteMessage(store, w, req)
					return
				}
				if req.Method == "POST" {
					ChangeMessageVisibilityTimeout(store, w, req)
					return
				}
			}
		}(w, req)

		fmt.Println(req.Method, req.URL.Path, time.Now().Sub(start))
	}
}
