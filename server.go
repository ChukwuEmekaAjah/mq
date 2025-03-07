package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/ChukwuEmekaAjah/mq/internal/handlers"
	"github.com/ChukwuEmekaAjah/mq/internal/util"
	"github.com/ChukwuEmekaAjah/mq/mq"
)

func main() {

	serverConfig := &util.ServerConfig{}

	flag.StringVar(&serverConfig.BackupBucket, "backupLocation", "./queues", "The location where the backup data is stored. It can be an S3 bucket where the queue data will be stored or a filesystem folder")
	flag.IntVar(&serverConfig.BackupFrequency, "backupFrequency", 10, "The number of seconds for which backups should be frequently made")
	flag.StringVar(&serverConfig.BackupType, "backupType", "fs", "The type of backup required to persist queue data. It can be: 's3' or 'fs'. It defaults to 'fs'")

	flag.Parse()

	store := mq.NewStore(serverConfig)

	mq.Restore(store, serverConfig)

	go func(db *mq.Store) {
		mq.Monitor(db)
	}(store)

	go func(db *mq.Store, config *util.ServerConfig) {
		mq.Backup(db, config)
	}(store, serverConfig)

	http.HandleFunc("/queues/{queueName}/messages/bulk", handlers.ManageMessages(store))
	http.HandleFunc("/queues/{queueName}/messages/{receiptHandle}", handlers.ManageMessages(store))
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
