package mq

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/ChukwuEmekaAjah/mq/internal/util"
)

const s3Backup string = "s3"
const fsBackup string = "fs"

// StorageManager enables different storage mediums to be used in backing up and restoring queue data
type StorageManager interface {
	Store(*ZipFile)
	Restore(*Store)
}

// FileStorageManager enables local filesystem backup
type FileStorageManager struct {
	location string
}

// Store persists the file in local filesystem
func (f *FileStorageManager) Store(file *ZipFile) {
	file.Close()
}

// Restore retrieves the queue data from the local filesystem and loads it into memory
func (f *FileStorageManager) Restore(store *Store) {
	_, err := os.Stat(f.location)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		fmt.Println("Nothing to restore because backup directory does not exist. Will be created in monitor goroutine.")
		return
	}

	entries, err := os.ReadDir(f.location)
	if err != nil {
		log.Fatal("Could not restore queue data while trying to read directory", err)
	}

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".zip") {
			continue
		}
		r, err := zip.OpenReader(path.Join(f.location, entry.Name()))
		defer r.Close()
		if err != nil {
			log.Fatal("Could not restore queue data", err)
		}

		zipFile := &ZipFile{reader: &r.Reader}
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

// ZipFile represents a collection of files to be compressed
type ZipFile struct {
	writer *zip.Writer
	reader *zip.Reader
	name   string
}

// WriteFile adds a new file to the zip file
func (z *ZipFile) WriteFile(name string, contents []byte) error {
	f, err := z.writer.Create(name)
	if err != nil {
		return err
	}

	_, err = f.Write(contents)
	if err != nil {
		return err
	}

	return nil
}

// ReadFile reads the contents of a zip file
func (z *ZipFile) ReadFile() (map[string]interface{}, error) {

	// Iterate through the files in the archive,
	// printing some of their contents.
	result := make(map[string]interface{})
	for _, f := range z.reader.File {
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}

		switch {
		case f.Name == "queue.json":
			q := &Queue{}
			buf := bytes.NewBuffer(nil)
			_, err := io.Copy(buf, rc)
			if err != nil {
				return nil, err
			}
			err = json.Unmarshal(buf.Bytes(), q)
			result["queue"] = q
			rc.Close()
			break
		case f.Name == "messages.json":
			messages := new([]Message)
			buf := bytes.NewBuffer(nil)
			_, err := io.Copy(buf, rc)
			if err != nil {
				return nil, err
			}
			err = json.Unmarshal(buf.Bytes(), messages)
			result["messages"] = messages
			rc.Close()
			break
		case f.Name == "read_messages.json":
			messages := new([]Message)
			buf := bytes.NewBuffer(nil)
			_, err := io.Copy(buf, rc)
			if err != nil {
				return nil, err
			}
			err = json.Unmarshal(buf.Bytes(), messages)
			result["readMessages"] = messages
			rc.Close()
			break
		}
	}

	return result, nil
}

// Close shuts down the zip file writer
func (z *ZipFile) Close() error {
	err := z.writer.Close()
	if err != nil {
		return err
	}

	return nil
}

// Backup tries to backup queue data based on set configuration
func Backup(store *Store, config *util.ServerConfig) {
	// each queue will have one directory
	// queue metadata will be its own json file
	// queue messages will be its own json file
	// received queue messages will be its own json file
	for {
		err := os.MkdirAll(config.BackupBucket, 0770)
		if err != nil {
			log.Fatal("Could not create backup directory", err)
		}

		store.Backup(config)
		time.Sleep(time.Second * time.Duration(config.BackupFrequency))
	}
}
