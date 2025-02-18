package mq

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ChukwuEmekaAjah/mq/internal/models"
	"github.com/google/uuid"
)

var queues sync.Map
var receivedMessagesQueues sync.Map
var receivedMessagesMap sync.Map

// Store holds the data for all queues that a user creates on the server
type Store struct {
	queues                 *sync.Map
	receivedMessagesQueues *sync.Map
	receivedMessagesMap    *sync.Map
}

// NewStore creates a new store pointer record
func NewStore() *Store {
	return &Store{
		queues:                 &sync.Map{},
		receivedMessagesQueues: &sync.Map{},
		receivedMessagesMap:    &sync.Map{},
	}
}

// CreateQueue creates a new queue
func (s *Store) CreateQueue(name string, attributes QueueAttributes, tags map[string]string) (string, error) {

	if val, ok := s.queues.Load(name); ok {
		return (val.(*Queue)).ID, nil
	}

	id := uuid.NewString()

	s.queues.Store(name, &Queue{
		ID:         id,
		QueueName:  name,
		Attributes: attributes,
		Tags:       tags,
	})

	s.receivedMessagesQueues.Store(name, NewDLLQueue(name, id))

	s.receivedMessagesMap.Store(name, &sync.Map{})

	return id, nil
}

// DeleteQueue deletes a queue and its contents from the system
func (s *Store) DeleteQueue(name string) (bool, error) {
	if _, ok := s.queues.Load(name); !ok {
		return false, errors.New("Queue does not exist")
	}

	s.queues.Delete(name)
	s.receivedMessagesQueues.Delete(name)
	s.receivedMessagesMap.Delete(name)

	return true, nil
}

// PurgeQueue deletes all the messages in a queue
func (s *Store) PurgeQueue(name string) (bool, error) {
	queueDB, ok := s.queues.Load(name)
	if !ok {
		return false, errors.New("Queue does not exist")
	}
	queueDB.(*Queue).Clear()

	receivedMessagesQueueDB, _ := s.receivedMessagesQueues.Load(name)
	receivedMessagesQueueDB.(*DLLQueue).Clear()

	return true, nil
}

// UpdateQueue updates fields in a queue and its contents from the system
func (s *Store) UpdateQueue(name string, data interface{}) (bool, error) {
	queue, ok := s.queues.Load(name)
	if !ok {
		return false, errors.New("Queue does not exist")
	}

	switch data.(type) {
	case QueueAttributes:
		queue.(*Queue).Attributes = data.(QueueAttributes)
	case map[string]string:
		queue.(*Queue).Tags = data.(map[string]string)
	case []string:
		for _, key := range data.([]string) {
			delete(queue.(*Queue).Tags, key)
		}
	}

	return true, nil
}

// GetQueue retrieves a queue's attributes
func (s *Store) GetQueue(name string) (*Queue, error) {
	queue, ok := s.queues.Load(name)
	if !ok {
		return nil, errors.New("Queue does not exist")
	}

	return queue.(*Queue), nil
}

// AddMessage adds a message to a queue
func (s *Store) AddMessage(queue string, message []byte) (string, error) {
	queueDB, ok := s.queues.Load(queue)
	if !ok {
		return "", errors.New("Queue does not exist")
	}

	var messageData *Message = &Message{}
	err := json.Unmarshal(message, messageData)

	if err != nil {
		return "", errors.New("Queue message could not be parsed")
	}

	if messageData.Body != "" && messageData.MD5OfBody != "" {
		if fmt.Sprintf("%x", md5.Sum([]byte(messageData.Body))) != messageData.MD5OfBody {
			return "", errors.New("Provided message body MD5 hash does not match")
		}
	}

	messageData.MessageID = uuid.NewString()
	messageData.InsertedAt = time.Now()
	messageData.Attributes = Attributes{
		ApproximateReceiveCount: 0,
		SentTimestamp:           time.Now().Unix(),
		SequenceNumber:          queueDB.(*Queue).Size + 1,
	}

	// update attributes
	// insert into queue
	queueDB.(*Queue).Enqueue(&QueueNode{
		Val: *messageData,
	})

	return messageData.MessageID, nil
}

// ReadMessages removes messages from the queue
func (s *Store) ReadMessages(queue string, size uint) ([]Message, error) {
	queueDB, ok := s.queues.Load(queue)
	if !ok {
		return nil, errors.New("Queue does not exist")
	}

	receivedMessagesQueueDB, _ := s.receivedMessagesQueues.Load(queue)
	receivedMessagesMapDB, _ := s.receivedMessagesMap.Load(queue)

	queueNodes, err := queueDB.(*Queue).DequeueBatch(int(size))

	result := make([]Message, 0, size)
	if err != nil {
		return nil, err
	}

	for _, queueNode := range queueNodes {
		queueNode.Val.Attributes.ApproximateFirstReceiveTimeStamp = int(time.Now().Unix())
		queueNode.Val.Attributes.ApproximateReceiveCount = queueNode.Val.Attributes.ApproximateReceiveCount + 1

		node := receivedMessagesQueueDB.(*DLLQueue).Enqueue(&DLLQueueNode{
			Val: queueNode.Val,
		})

		receivedMessagesMapDB.(*sync.Map).Store(queueNode.Val.ReceiptHandle, node)
		result = append(result, queueNode.Val)
	}

	return result, nil
}

// DeleteMessage removes messages from read messages queue
func (s *Store) DeleteMessage(queue, receiptHandle string) (bool, error) {

	receivedMessagesQueueDB, ok := s.receivedMessagesQueues.Load(queue)
	if !ok {
		return false, errors.New("Queue does not exist")
	}

	receivedMessagesMapDB, _ := s.receivedMessagesMap.Load(queue)
	node, ok := receivedMessagesMapDB.(*sync.Map).Load(receiptHandle)
	if !ok {
		return false, errors.New("Message receipt handle does not exist")
	}

	// Check if the message was read before the queue was last purged
	queueDB, _ := s.queues.Load(queue)
	if !queueDB.(*Queue).PurgedAt.IsZero() && node.(*DLLQueueNode).Val.ReadAt.Before(queueDB.(*Queue).PurgedAt) {
		node = nil
		return true, nil
	}

	err := receivedMessagesQueueDB.(*DLLQueue).Remove(node.(*DLLQueueNode))
	if err != nil {
		return false, errors.New("Message could not be deleted")
	}

	receivedMessagesMapDB.(*sync.Map).Delete(receiptHandle)
	return true, nil
}

// DeleteMessageBatch removes messages from read messages queue
func (s *Store) DeleteMessageBatch(queue string, entries []models.DeleteMessageBatchRequestEntry) models.DeleteMessageBatchResult {
	result := models.DeleteMessageBatchResult{
		Successful: make([]models.DeleteMessageBatchResultEntry, 0),
		Failed:     make([]models.BatchResultErrorEntry, 0),
	}

	receivedMessagesQueueDB, ok := s.receivedMessagesQueues.Load(queue)
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

	receivedMessagesMapDB, _ := s.receivedMessagesMap.Load(queue)

	for _, entry := range entries {

		node, ok := receivedMessagesMapDB.(*sync.Map).Load(entry.ReceiptHandle)

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
		queueDB, _ := s.queues.Load(queue)
		if !queueDB.(*Queue).PurgedAt.IsZero() && node.(*DLLQueueNode).Val.ReadAt.Before(queueDB.(*Queue).PurgedAt) {
			result.Successful = append(result.Successful, models.DeleteMessageBatchResultEntry{
				ID: entry.ID,
			})
			continue
		}

		err := receivedMessagesQueueDB.(*DLLQueue).Remove(node.(*DLLQueueNode))
		if err != nil {
			result.Failed = append(result.Failed, models.BatchResultErrorEntry{
				ID:          entry.ID,
				Message:     "Message could not be deleted",
				Code:        "5xx",
				SenderFault: false,
			})
			continue
		}

		receivedMessagesMapDB.(*sync.Map).Delete(entry.ReceiptHandle)
		result.Successful = append(result.Successful, models.DeleteMessageBatchResultEntry{
			ID: entry.ID,
		})
	}

	return result
}
