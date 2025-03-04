package mq

import (
	"archive/zip"
	"crypto/md5"
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/ChukwuEmekaAjah/mq/internal/models"
	"github.com/ChukwuEmekaAjah/mq/internal/util"
	"github.com/google/uuid"
)

var queues sync.Map
var receivedMessagesQueues sync.Map
var receivedMessagesMap sync.Map

// Store holds the data for all queues that a user creates on the server
type Store struct {
	queues                 map[string]*Queue
	receivedMessagesQueues map[string]*PriorityQueue
	receivedMessagesMap    map[string]map[string]*Item
	backupManager          StorageManager
	mu                     sync.Mutex
}

// NewStore creates a new store pointer record
func NewStore(config *util.ServerConfig) *Store {
	store := &Store{
		queues:                 make(map[string]*Queue),
		receivedMessagesQueues: make(map[string]*PriorityQueue),
		receivedMessagesMap:    make(map[string]map[string]*Item),
	}
	switch {
	case config.BackupType == util.FSBackup:
		store.backupManager = &FileStorageManager{
			location: config.BackupBucket,
		}
		break
	}
	return store
}

// CreateQueue creates a new queue
func (s *Store) CreateQueue(name string, attributes QueueAttributes, tags map[string]string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if val, ok := s.queues[name]; ok {
		return val.ID, nil
	}

	id := uuid.NewString()

	if attributes.VisibilityTimeout <= 0 {
		attributes.VisibilityTimeout = uint(MaxMessageVisibilityTimeout)
	}
	s.queues[name] = &Queue{
		ID:         id,
		QueueName:  name,
		Attributes: attributes,
		Tags:       tags,
	}

	s.receivedMessagesQueues[name] = NewPriorityQueue(name, id)

	s.receivedMessagesMap[name] = make(map[string]*Item)

	return id, nil
}

// DeleteQueue deletes a queue and its contents from the system
func (s *Store) DeleteQueue(name string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.queues[name]; !ok {
		return false, errors.New("Queue does not exist")
	}

	delete(s.queues, name)
	delete(s.receivedMessagesMap, name)
	delete(s.receivedMessagesQueues, name)

	return true, nil
}

// PurgeQueue deletes all the messages in a queue
func (s *Store) PurgeQueue(name string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.queues[name]; !ok {
		return false, errors.New("Queue does not exist")
	}

	s.queues[name].Clear()
	s.receivedMessagesQueues[name].Clear()
	s.receivedMessagesMap[name] = make(map[string]*Item)
	return true, nil
}

// UpdateQueue updates fields in a queue and its contents from the system
func (s *Store) UpdateQueue(name string, data interface{}) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.queues[name]; !ok {
		return false, errors.New("Queue does not exist")
	}

	switch data.(type) {
	case QueueAttributes:
		s.queues[name].Attributes = data.(QueueAttributes)
	case map[string]string:
		s.queues[name].Tags = data.(map[string]string)
	case []string:
		for _, key := range data.([]string) {
			delete(s.queues[name].Tags, key)
		}
	}

	return true, nil
}

// GetQueue retrieves a queue's attributes
func (s *Store) GetQueue(name string) (*Queue, error) {

	queue, ok := s.queues[name]
	if !ok {
		return nil, errors.New("Queue does not exist")
	}

	return queue, nil
}

// ListQueues retrieves all the queues on the server
func (s *Store) ListQueues() []Queue {
	result := make([]Queue, 0)

	for _, q := range s.queues {
		result = append(result, Queue{
			Attributes: q.Attributes,
			QueueName:  q.QueueName,
			ID:         q.ID,
			Tags:       q.Tags,
		})
	}

	return result
}

// AddMessage adds a message to a queue
func (s *Store) AddMessage(queue string, message *models.MessageRequest) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	queueDB, ok := s.queues[queue]
	if !ok {
		return "", errors.New("Queue does not exist")
	}

	md5OfMessageBody := fmt.Sprintf("%x", md5.Sum([]byte(message.MessageBody)))
	messageID := uuid.NewString()
	var messageData Message = Message{
		MessageID:  messageID,
		InsertedAt: time.Now(),
		Attributes: Attributes{
			ApproximateReceiveCount: 0,
			SentTimestamp:           time.Now().Unix(),
			SequenceNumber:          queueDB.Size + 1,
		},
		Body:              message.MessageBody,
		MD5OfBody:         md5OfMessageBody,
		MessageAttributes: message.MessageAttributes,
	}

	// insert into queue
	queueDB.Enqueue(&QueueNode{
		Val: messageData,
	})

	return messageData.MessageID, nil
}

// AddMessageBatch adds a set of messages at once into the message queue
func (s *Store) AddMessageBatch(queue string, messages []models.MessageRequest) models.BatchResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := models.BatchResult{Successful: make([]map[string]string, 0), Failed: []models.BatchResultErrorEntry{}}
	queueDB, ok := s.queues[queue]
	if !ok {
		for _, entry := range messages {
			result.Failed = append(result.Failed, models.BatchResultErrorEntry{
				ID:          entry.ID,
				Message:     "Queue does not exist",
				Code:        "4xx",
				SenderFault: true,
			})
		}
		return result
	}

	validMessages := make([]models.MessageRequest, 0)
	for _, entry := range messages {

		if entry.MessageBody == "" {
			result.Failed = append(result.Failed, models.BatchResultErrorEntry{
				ID:          entry.ID,
				Message:     "Message body is required",
				Code:        "4xx",
				SenderFault: true,
			})
			continue
		}

		if !ok {
			result.Failed = append(result.Failed, models.BatchResultErrorEntry{
				ID:          entry.ID,
				Message:     "Message receipt handle does not exist",
				Code:        "4xx",
				SenderFault: true,
			})
			continue
		}

		validMessages = append(validMessages, entry)
	}

	messagesToInsert := make([]*QueueNode, 0)
	for index, message := range validMessages {
		md5OfMessageBody := fmt.Sprintf("%x", md5.Sum([]byte(message.MessageBody)))
		messageID := uuid.NewString()
		sequenceNumber := queueDB.Size + uint(index) + 1
		messagesToInsert = append(messagesToInsert, &QueueNode{
			Val: Message{
				MessageID:  messageID,
				InsertedAt: time.Now(),
				Attributes: Attributes{
					ApproximateReceiveCount: 0,
					SentTimestamp:           time.Now().Unix(),
					SequenceNumber:          sequenceNumber,
				},
				Body:              message.MessageBody,
				MD5OfBody:         md5OfMessageBody,
				MessageAttributes: message.MessageAttributes,
			},
		})
		result.Successful = append(result.Successful, map[string]string{
			"Id":               message.ID,
			"MD5OfMessageBody": md5OfMessageBody,
			"MessageId":        messageID,
			"SequenceNumber":   strconv.FormatUint(uint64(sequenceNumber), 10),
		})
	}
	queueDB.EnqueueBatch(messagesToInsert)

	return result
}

// ReadMessages removes messages from the queue
func (s *Store) ReadMessages(queue string, size uint) ([]Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	queueDB, ok := s.queues[queue]
	if !ok {
		return nil, errors.New("Queue does not exist")
	}

	receivedMessagesQueueDB, _ := s.receivedMessagesQueues[queue]
	receivedMessagesMapDB, _ := s.receivedMessagesMap[queue]

	queueNodes, err := queueDB.DequeueBatch(int(size))

	result := make([]Message, 0, size)
	if err != nil {
		return nil, err
	}

	messageVisibilityTimeout := queueDB.Attributes.VisibilityTimeout

	for _, queueNode := range queueNodes {
		queueNode.Val.Attributes.ApproximateFirstReceiveTimeStamp = int(time.Now().Unix())
		queueNode.Val.Attributes.ApproximateReceiveCount = queueNode.Val.Attributes.ApproximateReceiveCount + 1
		queueNode.Val.ReadAt = time.Now()
		queueNode.Val.MessageVisibilityTimesOutAt = time.Now().Add(time.Duration(messageVisibilityTimeout) * time.Second)
		node := receivedMessagesQueueDB.Enqueue(&Item{val: queueNode.Val})

		receivedMessagesMapDB[queueNode.Val.ReceiptHandle] = node
		result = append(result, queueNode.Val)
	}

	return result, nil
}

// DeleteMessage removes messages from read messages queue
func (s *Store) DeleteMessage(queue, receiptHandle string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	receivedMessagesQueueDB, ok := s.receivedMessagesQueues[queue]
	if !ok {
		return false, errors.New("Queue does not exist")
	}

	node, ok := s.receivedMessagesMap[queue][receiptHandle]
	if !ok {
		return false, errors.New("Message receipt handle does not exist")
	}

	delete(s.receivedMessagesMap[queue], receiptHandle)

	// Check if the message was read before the queue was last purged
	queueDB, _ := s.queues[queue]
	if !queueDB.PurgedAt.IsZero() && node.val.ReadAt.Before(queueDB.PurgedAt) {
		node = nil
		return true, nil
	}

	// will not need this again once we migrate to PriorityQueue based on when message was read
	err := receivedMessagesQueueDB.Remove(node)
	if err != nil {
		return false, errors.New("Message could not be deleted")
	}

	return true, nil
}

// DeleteMessageBatch removes messages from read messages queue
func (s *Store) DeleteMessageBatch(queue string, entries []models.DeleteMessageBatchRequestEntry) models.BatchResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := models.BatchResult{
		Successful: make([]map[string]string, 0),
		Failed:     make([]models.BatchResultErrorEntry, 0),
	}

	receivedMessagesQueueDB, ok := s.receivedMessagesQueues[queue]
	if !ok {
		for _, entry := range entries {
			result.Failed = append(result.Failed, models.BatchResultErrorEntry{
				ID:          entry.ID,
				Message:     "Queue does not exist",
				Code:        "4xx",
				SenderFault: true,
			})
		}
		return result
	}

	receivedMessagesMapDB, _ := s.receivedMessagesMap[queue]

	for _, entry := range entries {

		node, ok := receivedMessagesMapDB[entry.ReceiptHandle]

		if !ok {
			result.Failed = append(result.Failed, models.BatchResultErrorEntry{
				ID:          entry.ID,
				Message:     "Message receipt handle does not exist",
				Code:        "4xx",
				SenderFault: true,
			})
			continue
		}

		// Check if the message was read before the queue was last purged
		queueDB, _ := s.queues[queue]
		if !queueDB.PurgedAt.IsZero() && node.val.ReadAt.Before(queueDB.PurgedAt) {
			result.Successful = append(result.Successful, map[string]string{
				"ID": entry.ID,
			})
			continue
		}

		err := receivedMessagesQueueDB.Remove(node)
		if err != nil {
			result.Failed = append(result.Failed, models.BatchResultErrorEntry{
				ID:          entry.ID,
				Message:     "Message could not be deleted",
				Code:        "5xx",
				SenderFault: false,
			})
			continue
		}

		delete(s.receivedMessagesMap[queue], entry.ReceiptHandle)

		result.Successful = append(result.Successful, map[string]string{
			"ID": entry.ID,
		})
	}

	return result
}

// UpdateMessage updates specific fields in a message
func (s *Store) UpdateMessage(queue string, data interface{}) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.queues[queue]
	if !ok {
		return false, errors.New("Queue does not exist")
	}

	switch data.(type) {
	case models.ChangeMessageVisibilityRequest:
		if data.(models.ChangeMessageVisibilityRequest).VisibilityTimeout < 0 {
			return false, errors.New("Message visibility timeout cannot be less than 0")
		}

		node, ok := s.receivedMessagesMap[queue][data.(models.ChangeMessageVisibilityRequest).ReceiptHandle]
		if !ok {
			return false, errors.New("Message receipt handle does not exist")
		}

		node.val.MessageVisibilityTimesOutAt = time.Now().Add(time.Duration(data.(models.ChangeMessageVisibilityRequest).VisibilityTimeout) * time.Second)
		s.receivedMessagesQueues[queue].Update(node)
	}

	return true, nil
}

// UpdateMessageBatch updates specific fields in a message
func (s *Store) UpdateMessageBatch(queue string, entries []models.ChangeMessageVisibilityRequest) models.BatchResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := models.BatchResult{
		Successful: make([]map[string]string, 0),
		Failed:     make([]models.BatchResultErrorEntry, 0),
	}
	_, ok := s.queues[queue]
	if !ok {
		for _, entry := range entries {
			result.Failed = append(result.Failed, models.BatchResultErrorEntry{
				ID:          entry.ID,
				Message:     "Queue does not exist",
				Code:        "4xx",
				SenderFault: true,
			})
		}
		return result
	}

	for _, entry := range entries {
		if entry.VisibilityTimeout < 0 {
			result.Failed = append(result.Failed, models.BatchResultErrorEntry{
				ID:          entry.ID,
				Message:     "Message visibility timeout cannot be less than 0",
				Code:        "4xx",
				SenderFault: true,
			})
			continue
		}

		node, ok := s.receivedMessagesMap[queue][entry.ReceiptHandle]
		if !ok {
			result.Failed = append(result.Failed, models.BatchResultErrorEntry{
				ID:          entry.ID,
				Message:     "Message receipt handle does not exist",
				Code:        "4xx",
				SenderFault: true,
			})
			continue
		}

		node.val.MessageVisibilityTimesOutAt = time.Now().Add(time.Duration(entry.VisibilityTimeout) * time.Second)
		result.Successful = append(result.Successful, map[string]string{
			"ID": entry.ID,
		})
	}

	return result
}

// Backup writes the store data to a non-volatile storage
func (s *Store) Backup(config *util.ServerConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for queueName := range s.queues {

		qBytes, err := s.queues[queueName].ToJSON()

		if err != nil {
			continue
		}

		messageBytes, err := s.queues[queueName].MessagesToJSON()
		if err != nil {
			continue
		}

		readMessagesBytes, err := s.receivedMessagesQueues[queueName].MessagesToJSON()

		buffer, err := os.Create(path.Join(config.BackupBucket, fmt.Sprintf("%s_%s.zip", config.BackupBucket, queueName)))

		if err != nil {
			continue
		}
		zipFile := &ZipFile{writer: zip.NewWriter(buffer)}
		zipFile.WriteFile("queue.json", qBytes)
		zipFile.WriteFile("messages.json", messageBytes)
		zipFile.WriteFile("read_messages.json", readMessagesBytes)
		s.backupManager.Store(zipFile)
	}
}

// Monitor restores received messages that haven't been deleted back into the main message queue
func (s *Store) Monitor() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for queue := range s.receivedMessagesQueues {
		for head := s.receivedMessagesQueues[queue].Front(); head != nil; head = s.receivedMessagesQueues[queue].Front() {
			if head.val.MessageVisibilityTimesOutAt.Before(time.Now()) {
				item, _ := s.receivedMessagesQueues[queue].Dequeue()
				delete(s.receivedMessagesMap[queue], item.val.ReceiptHandle)

				s.queues[queue].Enqueue(&QueueNode{
					Val: item.val,
				})
				item = nil
			} else {
				// if the front message is not expired, other messages behind it won't be expired
				break
			}
		}
	}
}
