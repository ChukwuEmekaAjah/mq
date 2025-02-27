package mq

import (
	"archive/zip"
	"log"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/ChukwuEmekaAjah/mq/internal/util"
)

// Restore tries to read the queue backup data into memory when the server starts
func Restore(store *Store, config *util.ServerConfig) {
	entries, err := os.ReadDir(config.BackupBucket)
	if err != nil {
		log.Fatal("Could not restore queue data while trying to read directory", err)
	}

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".zip") {
			continue
		}
		r, err := zip.OpenReader(path.Join(config.BackupBucket, entry.Name()))
		if err != nil {
			log.Fatal("Could not restore queue data", err)
		}

		zipFile := &ZipFile{reader: r}
		result, err := zipFile.ReadFile()
		if err != nil {
			log.Fatal("Could not restore queue data", err)
		}

		q := result["queue"]
		queue := q.(*Queue)
		unreadMessages := result["messages"]
		messages := unreadMessages.(*[]Message)
		messagesQueueNodes := make([]*QueueNode, 0)
		for _, message := range *messages {
			messagesQueueNodes = append(messagesQueueNodes, &QueueNode{Val: message})
		}
		queue.EnqueueBatch(messagesQueueNodes)
		store.queues.Store(queue.QueueName, queue)
		receivedMessagesQueue := NewDLLQueue(queue.QueueName, queue.ID)
		readMessages := result["readMessages"]
		messages = readMessages.(*[]Message)
		readMessagesQueueNodes := make([]*DLLQueueNode, 0)
		readMessagesMap := &sync.Map{}
		for _, message := range *messages {
			readMessageNode := &DLLQueueNode{Val: message}
			readMessagesQueueNodes = append(readMessagesQueueNodes, readMessageNode)
			readMessagesMap.Store(message.ReceiptHandle, readMessageNode)
		}
		store.receivedMessagesQueues.Store(queue.QueueName, receivedMessagesQueue)
		store.receivedMessagesMap.Store(queue.QueueName, readMessagesMap)
	}
}
