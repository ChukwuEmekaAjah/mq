package mq

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3StoreManager represents a backup system that uses S3
type S3StoreManager struct {
	bucket string
	client *s3.Client
}

// Setup instantiates S3 client for interacting with s3 service
func (s *S3StoreManager) Setup() {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(os.Getenv("AWS_REGION")))

	if err != nil {
		log.Fatal(err)
	}

	s.client = s3.NewFromConfig(cfg)
}

// Store persists the queue data in file to remote S3 bucket
// update key of backup file stored in S3
func (s *S3StoreManager) Store(file *ZipFile) {
	file.writer.Close()
	f, err := os.Open(file.name)
	if err != nil {
		fmt.Println("Zip file location could not be opened")
		log.Fatal(err)
	}

	fileName := fmt.Sprintf("queues/%s", path.Base(file.name))
	_, err = s.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &fileName,
		Body:   f,
	})

	if err != nil {
		fmt.Println("Queue data could not be backed up", err)
		return
	}
}

// Restore retrieves the queue data from the remote S3 storage and loads it into memory
func (s *S3StoreManager) Restore(store *Store) {
	output, err := s.client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: &s.bucket,
		Prefix: aws.String("queues/"),
	})

	if err != nil {
		log.Fatal(err)
	}

	for _, queueZipFile := range output.Contents {
		object, err := s.client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: &s.bucket,
			Key:    queueZipFile.Key,
		})

		if err != nil {
			fmt.Println("Queue data object could not be retrieved for ", queueZipFile.Key, "because of ", err)
			continue
		}

		content, err := io.ReadAll(object.Body)
		if err != nil {
			fmt.Println("Queue data object could not be read for ", queueZipFile.Key, "because of ", err)
			continue
		}

		r, err := zip.NewReader(bytes.NewReader(content), int64(len(content)))
		if err != nil {
			fmt.Println("Could not unzip file because of ", err)
			continue
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
		store.queues[queue.QueueName] = queue
		receivedMessagesQueue := NewPriorityQueue(queue.QueueName, queue.ID)
		readMessages := result["readMessages"]
		messages = readMessages.(*[]Message)
		readMessagesQueueNodes := make([]*Item, 0)
		readMessagesMap := make(map[string]*Item)
		for _, message := range *messages {
			readMessageNode := &Item{val: message}
			readMessagesQueueNodes = append(readMessagesQueueNodes, readMessageNode)
			readMessagesMap[message.ReceiptHandle] = readMessageNode
		}
		store.receivedMessagesQueues[queue.QueueName] = receivedMessagesQueue
		store.receivedMessagesMap[queue.QueueName] = readMessagesMap
	}
}
