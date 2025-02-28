package mq

import (
	"testing"

	"github.com/google/uuid"
)

func TestQueueEnqueue(t *testing.T) {
	q := &Queue{Size: 0}

	q.Enqueue(&QueueNode{
		Val: Message{
			MessageID: uuid.NewString(),
			Body:      string("Hello world"),
		},
	})
	q.Enqueue(&QueueNode{
		Val: Message{
			MessageID: uuid.NewString(),
			Body:      string("Hello world 2"),
		},
	})

	q.Enqueue(&QueueNode{
		Val: Message{
			MessageID: uuid.NewString(),
			Body:      string("Hello world 3"),
		},
	})

	if q.Size != 3 {
		t.Errorf("Expected q size to be %d but got %d", 3, q.Size)
	}
}

func TestQueueDequeue(t *testing.T) {
	q := &Queue{Size: 0}

	q.Enqueue(&QueueNode{
		Val: Message{
			MessageID: uuid.NewString(),
			Body:      string("Hello world"),
		},
	})
	q.Enqueue(&QueueNode{
		Val: Message{
			MessageID: uuid.NewString(),
			Body:      string("Hello world 2"),
		},
	})

	q.Enqueue(&QueueNode{
		Val: Message{
			MessageID: uuid.NewString(),
			Body:      string("Hello world 3"),
		},
	})

	if q.Size != 3 {
		t.Errorf("Expected q size to be %d but got %d", 3, q.Size)
	}

	q.Dequeue()
	if q.Size != 2 {
		t.Errorf("Expected q size to be %d but got %d", 2, q.Size)
	}
	q.Dequeue()
	if q.Size != 1 {
		t.Errorf("Expected q size to be %d but got %d", 1, q.Size)
	}
	q.Dequeue()
	if q.Size != 0 {
		t.Errorf("Expected q size to be %d but got %d", 0, q.Size)
	}

	node, err := q.Dequeue()

	if err == nil {
		t.Errorf("Expected q to return error for empty queue but got %v", node)
	}
}
