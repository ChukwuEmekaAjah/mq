package mq

import (
	"container/heap"
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

// Item represents a node in the queue
type Item struct {
	val   Message
	index int
}

// PriorityQueueContainer is an abstraction on top a slice that contains a list of messages
type PriorityQueueContainer []*Item

// Len returns the length of the priority queue container
func (pqc PriorityQueueContainer) Len() int {
	return len(pqc)
}

// Less checks if one value is greater than the other
func (pqc PriorityQueueContainer) Less(i, j int) bool {
	return pqc[i].val.MessageVisibilityTimesOutAt.Before(pqc[j].val.MessageVisibilityTimesOutAt)
}

// Swap updates the position of an item in the queue
func (pqc PriorityQueueContainer) Swap(i, j int) {
	pqc[i], pqc[j] = pqc[j], pqc[i]
	pqc[i].index = i
	pqc[j].index = j
}

// Push adds an item to the queue
func (pqc *PriorityQueueContainer) Push(msg any) {
	size := len(*pqc)
	item := msg.(*Item)
	item.index = size
	*pqc = append(*pqc, item)
}

// get retrieves item at given position
func (pqc PriorityQueueContainer) get(index int) *Item {
	return pqc[index]
}

// Pop removes the oldest read message from the queue
func (pqc *PriorityQueueContainer) Pop() any {
	old := *pqc
	size := len(old)
	item := old[size-1]
	old[size-1] = nil
	item.index = -1 // for safety
	*pqc = old[0 : size-1]
	return item
}

// PriorityQueue represents a collection for managing read messages and
// keeping them in order based on the time the message was read
type PriorityQueue struct {
	name      string
	id        string
	mu        sync.Mutex
	container *PriorityQueueContainer
}

// NewPriorityQueue creates and returns a new instance of a priority queue
func NewPriorityQueue(name, id string) *PriorityQueue {
	return &PriorityQueue{
		name:      name,
		id:        id,
		container: new(PriorityQueueContainer),
	}
}

// Enqueue adds a message to the queue
func (pq *PriorityQueue) Enqueue(msg *Item) *Item {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	heap.Push(pq.container, msg)
	return msg
}

// EnqueueBatch adds a group of messages to the queue
func (pq *PriorityQueue) EnqueueBatch(msgs []*Item) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	for _, msg := range msgs {
		heap.Push(pq.container, msg)
	}
}

// Dequeue removes a message from the head of the queue
func (pq *PriorityQueue) Dequeue() (*Item, error) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.container.Len() < 1 {
		return nil, errors.New("Queue is empty")
	}

	item := heap.Pop(pq.container)

	return item.(*Item), nil
}

// Remove deletes a node from its position in the queue
func (pq *PriorityQueue) Remove(node *Item) error {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.container.Len() < 1 {
		return errors.New("Queue is empty")
	}

	heap.Remove(pq.container, node.index)
	return nil
}

// Front returns element in front of the queue
func (pq *PriorityQueue) Front() *Item {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.container.Len() < 1 {
		return nil
	}

	return pq.container.get(0)
}

// Clear removes all the messages in the queue
func (pq *PriorityQueue) Clear() bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pq.container = new(PriorityQueueContainer)
	return true
}

// Update organizes the heap array after an item is changed so as to maintain order
func (pq *PriorityQueue) Update(item *Item) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	heap.Fix(pq.container, item.index)
	return true
}

// MessagesToJSON converts all queue messages to a json array
func (pq *PriorityQueue) MessagesToJSON() ([]byte, error) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	result := make([]Message, 0, pq.container.Len())
	for _, item := range *pq.container {
		result = append(result, item.val)
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
