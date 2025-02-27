package mq

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

// QueueNode represents a message item in the message queue
type QueueNode struct {
	Val  Message
	Next *QueueNode
}

// DLLQueueNode represents a message item in the message queue
type DLLQueueNode struct {
	Val  Message
	Next *DLLQueueNode
	Prev *DLLQueueNode
}

// DLLQueue represents a doubly-linked queue
type DLLQueue struct {
	Name string
	ID   string
	Head *DLLQueueNode
	Tail *DLLQueueNode
	Size uint
	mu   sync.Mutex
}

// NewDLLQueue creates a new DLLQueue and returns a reference to the pointer
func NewDLLQueue(name, id string) *DLLQueue {
	head := &DLLQueueNode{}
	tail := &DLLQueueNode{}
	tail.Prev = head
	head.Next = tail
	return &DLLQueue{
		Name: name,
		ID:   id,
		Head: head,
		Tail: tail,
		Size: 0,
	}
}

// Enqueue adds a message to the queue
func (q *DLLQueue) Enqueue(message *DLLQueueNode) *DLLQueueNode {
	q.mu.Lock()
	defer q.mu.Unlock()

	message.Prev = q.Tail.Prev
	message.Next = q.Tail
	q.Tail.Prev.Next = message
	q.Tail.Prev = message

	q.Size++

	return message

}

// EnqueueBatch adds a group of messages to the queue
func (q *DLLQueue) EnqueueBatch(messages []*DLLQueueNode) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, message := range messages {
		message.Prev = q.Tail.Prev
		message.Next = q.Tail
		q.Tail.Prev.Next = message
		q.Tail.Prev = message

		q.Size++
	}

	return nil

}

// Dequeue removes a message from the head of the queue
func (q *DLLQueue) Dequeue() (*DLLQueueNode, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.Size < 1 {
		return nil, errors.New("Queue is empty")
	}

	temp := q.Head.Next
	q.Head.Next = temp.Next
	temp.Next.Prev = q.Head

	q.Size--

	return temp, nil
}

// Remove deletes a node from its position in the queue
func (q *DLLQueue) Remove(node *DLLQueueNode) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.Size < 1 {
		return errors.New("Queue is empty")
	}

	node.Prev.Next = node.Next
	node.Next.Prev = node.Prev
	node.Prev = nil
	node.Next = nil
	q.Size--

	return nil
}

// Front returns element in front of the queue
func (q *DLLQueue) Front() *DLLQueueNode {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.Size < 1 {
		return nil
	}

	return q.Head.Next
}

// Clear removes all the messages in the queue
func (q *DLLQueue) Clear() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.Size = 0
	q.Head.Next = q.Tail
	q.Tail.Prev = q.Head

	return true
}

// MessagesToJSON converts all queue messages to a json array
func (q *DLLQueue) MessagesToJSON() ([]byte, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	result := make([]Message, 0, q.Size)

	current := q.Head.Next
	for current != q.Tail {
		result = append(result, current.Val)
		current = current.Next
	}

	messageBytes, err := json.Marshal(result)

	if err != nil {
		return []byte{}, err
	}

	return messageBytes, nil
}

// QueueAttributes represents the attributes of a queue
type QueueAttributes struct {
	DelaySeconds                  uint
	MaxMessageSize                int
	MessageRetentionPeriod        uint
	ReceiveMessageWaitTimeSeconds uint
	VisibilityTimeout             uint
}

// Queue represents the abstraction of a queue data structure
type Queue struct {
	ID         string
	Head       *QueueNode `json:"-"`
	Tail       *QueueNode `json:"-"`
	Size       uint       `json:"-"`
	mu         sync.Mutex
	QueueName  string
	Attributes QueueAttributes
	Tags       map[string]string
	PurgedAt   time.Time `json:"-"`
}

// Dequeue removes a message from the head of the queue
func (q *Queue) Dequeue() (*QueueNode, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.Size < 1 {
		return nil, errors.New("Queue is empty")
	}

	temp := q.Head
	q.Head = q.Head.Next
	q.Size--

	temp.Next = nil
	return temp, nil
}

// DequeueBatch removes at most the given size of elements from the queue
func (q *Queue) DequeueBatch(size int) ([]*QueueNode, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	result := make([]*QueueNode, 0, size)

	for q.Size > 0 && len(result) < size {

		temp := q.Head
		q.Head = q.Head.Next
		q.Size--

		temp.Next = nil
		temp.Val.ReadAt = time.Now()
		temp.Val.ReceiptHandle = uuid.NewString()
		result = append(result, temp)
	}

	return result, nil
}

// Clear removes all the messages in the queue
func (q *Queue) Clear() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.Size = 0
	q.Head = nil
	q.Tail = nil
	q.PurgedAt = time.Now()

	return true
}

// Enqueue adds a message to the queue
func (q *Queue) Enqueue(message *QueueNode) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.Size == 0 {
		q.Head = message
		q.Tail = message
		q.Size++
		return nil
	}

	q.Tail.Next = message
	q.Tail = message
	q.Size++
	return nil

}

// EnqueueBatch adds a group of messages to the queue
func (q *Queue) EnqueueBatch(messages []*QueueNode) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(messages) == 0 {
		return nil
	}
	if q.Size == 0 {
		q.Head = messages[0]
		q.Tail = messages[0]
		q.Size++
	}

	for _, message := range messages[1:] {
		q.Tail.Next = message
		q.Tail = message
		q.Size++
	}

	return nil
}

// ToJSON converts the queue struct to a json string
func (q *Queue) ToJSON() ([]byte, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	qBytes, err := json.Marshal(q)

	if err != nil {
		return []byte{}, err
	}

	return qBytes, nil
}

// MessagesToJSON converts the queue messages struct to json
func (q *Queue) MessagesToJSON() ([]byte, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	result := make([]Message, 0, q.Size)

	current := q.Head
	for current != nil {
		result = append(result, current.Val)
		current = current.Next
	}

	messageBytes, err := json.Marshal(result)

	if err != nil {
		return []byte{}, err
	}

	return messageBytes, nil
}
