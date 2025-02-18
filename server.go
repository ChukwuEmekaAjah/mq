package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/ChukwuEmekaAjah/mq/internal/handlers"
	"github.com/ChukwuEmekaAjah/mq/mq"
)

func main() {

	store := mq.NewStore()

	go func(db *mq.Store) {
		mq.Monitor(db)
	}(store)

	http.HandleFunc("/queues/{queueName}/messages/{receiptHandleId}", handlers.ManageMessages(store))
	http.HandleFunc("/queues/{queueName}/messages", handlers.ManageMessages(store))
	http.HandleFunc("/queues/{queueName}/attributes", handlers.ManageQueues(store))
	http.HandleFunc("/queues/{queueName}/tags", handlers.ManageQueues(store))
	http.HandleFunc("/queues", handlers.ManageQueues(store))
	http.HandleFunc("/queues/{queueName}", handlers.ManageQueues(store))

	port := os.Getenv("PORT")
	if port == "" {
		port = ":80"
	}
	fmt.Println("Starting server on port", port)
	http.ListenAndServe(port, nil)
}
