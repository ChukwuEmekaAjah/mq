package handlers

import (
	"fmt"
	"net/http"
	"time"

	"github.com/ChukwuEmekaAjah/mq/mq"
)

// ManageQueues manages queues
func ManageQueues(store *mq.Store) func(http.ResponseWriter, *http.Request) {

	return func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()
		func(w http.ResponseWriter, req *http.Request) {
			w.Header().Add("Content-Type", "application/json")

			if req.Pattern == "/queues" {
				if req.Method == "POST" {
					CreateQueue(store, w, req)
					return
				}

				if req.Method == "GET" {
					ListQueues(store, w, req)
					return
				}
			}

			if req.Pattern == "/queues/{queueName}" && req.Method == "DELETE" {
				DeleteQueue(store, w, req)
				return
			}

			if req.Pattern == "/queues/{queueName}" && req.Method == "GET" {
				GetQueue(store, w, req)
				return
			}

			if req.Pattern == "/queues/{queueName}/attributes" && req.Method == "PUT" {
				SetQueueAttributes(store, w, req)
				return
			}

			if req.Pattern == "/queues/{queueName}/tags" && req.Method == "PUT" {
				TagQueue(store, w, req)
				return
			}

			if req.Pattern == "/queues/{queueName}/tags" && req.Method == "DELETE" {
				UntagQueue(store, w, req)
				return
			}
		}(w, req)

		fmt.Println(req.Method, req.URL.Path, time.Now().Sub(start))
	}
}
