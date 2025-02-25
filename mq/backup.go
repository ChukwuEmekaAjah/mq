package mq

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ChukwuEmekaAjah/mq/internal/util"
)

// StorageManager enables different storage mediums to be used in backing up and restoring queue data
type StorageManager interface {
	Store(*ZipFile)
	Restore(string) *io.ReadWriteCloser
}

// FileStorageManager enables local filesystem backup
type FileStorageManager struct {
	location string
}

// Store persists the file in local filesystem
func (f *FileStorageManager) Store(file *ZipFile) {
	file.Close()
}

// ZipFile represents a collection of files to be compressed
type ZipFile struct {
	writer *zip.Writer
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
	fmt.Println(fmt.Sprintf("server config is %+v", *config))
	// each queue will have one directory
	// queue metadata will be its own json file
	// queue messages will be its own json file
	// received queue messages will be its own json file
	for {
		queueNames := make([]string, 0)
		store.queues.Range(func(queueName, queue interface{}) bool {
			queueNames = append(queueNames, queue.(*Queue).QueueName)
			return true
		})
		backupManager := &FileStorageManager{
			location: "queue",
		}

		for _, queueName := range queueNames {
			q, exists := store.queues.Load(queueName)

			if !exists {
				continue
			}

			qBytes, err := q.(*Queue).ToJSON()

			if err != nil {
				continue
			}

			messageBytes, err := q.(*Queue).MessagesToJSON()
			if err != nil {
				continue
			}

			readQ, exists := store.receivedMessagesQueues.Load(queueName)
			if !exists {
				continue
			}

			readMessagesBytes, err := readQ.(*DLLQueue).MessagesToJSON()

			buffer, _ := os.Create(fmt.Sprintf("%s_%s.zip", backupManager.location, queueName))
			zipFile := &ZipFile{writer: zip.NewWriter(buffer)}
			zipFile.WriteFile("queue.json", qBytes)
			zipFile.WriteFile("messages.json", messageBytes)
			zipFile.WriteFile("read_messages.json", readMessagesBytes)
			backupManager.Store(zipFile)
		}

		time.Sleep(time.Second * time.Duration(config.BackupFrequency))
	}
}
