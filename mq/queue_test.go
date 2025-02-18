package mq

import (
	"testing"

	"github.com/google/uuid"
)

func TestDLLQueueEnqueue(t *testing.T) {
	q := NewDLLQueue("MyQueue", "id")

	q.Enqueue(&DLLQueueNode{
		Val: Message{
			MessageID: uuid.NewString(),
			Body:      string("Hello world"),
		},
	})
	node2 := q.Enqueue(&DLLQueueNode{
		Val: Message{
			MessageID: uuid.NewString(),
			Body:      string("Hello world 2"),
		},
	})

	node3 := q.Enqueue(&DLLQueueNode{
		Val: Message{
			MessageID: uuid.NewString(),
			Body:      string("Hello world 3"),
		},
	})

	if node3.Prev != node2 {
		t.Errorf("Expected node 3 prev node to be %+v but got %+v", node2, node3.Prev)
	}

	if q.Size != 3 {
		t.Errorf("Expected q size to be %d but got %d", 3, q.Size)
	}
}

func TestDLLQueueDequeue(t *testing.T) {
	q := NewDLLQueue("MyQueue", "id")

	q.Enqueue(&DLLQueueNode{
		Val: Message{
			MessageID: uuid.NewString(),
			Body:      string("Hello world"),
		},
	})
	q.Enqueue(&DLLQueueNode{
		Val: Message{
			MessageID: uuid.NewString(),
			Body:      string("Hello world 2"),
		},
	})

	q.Enqueue(&DLLQueueNode{
		Val: Message{
			MessageID: uuid.NewString(),
			Body:      string("Hello world 3"),
		},
	})

	q.Dequeue()
	if q.Size != 2 {
		t.Errorf("Expected q size to be %d but got %d", 2, q.Size)
	}

	q.Dequeue()
	if q.Size != 1 {
		t.Errorf("Expected q size to be %d but got %d", 1, q.Size)
	}
}

func TestDLLQueueRemove(t *testing.T) {
	q := NewDLLQueue("MyQueue", "id")

	node1 := q.Enqueue(&DLLQueueNode{
		Val: Message{
			MessageID: uuid.NewString(),
			Body:      string("Hello world"),
		},
	})
	node2 := q.Enqueue(&DLLQueueNode{
		Val: Message{
			MessageID: uuid.NewString(),
			Body:      string("Hello world 2"),
		},
	})

	node3 := q.Enqueue(&DLLQueueNode{
		Val: Message{
			MessageID: uuid.NewString(),
			Body:      string("Hello world 3"),
		},
	})

	q.Remove(node2)
	if node3.Prev != node1 {
		t.Errorf("Expected node 3 prev node to be %+v but got %+v", node1, node3.Prev)
	}

	if q.Size != 2 {
		t.Errorf("Expected queue size to be 2 but got %d", q.Size)
	}

	q.Remove(node1)
	if node3.Prev == node1 {
		t.Errorf("Expected node 3 prev node to not be %+v but got %+v", node1, node3.Prev)
	}

	if q.Size != 1 {
		t.Errorf("Expected queue size to be 1 but got %d", q.Size)
	}

	q.Remove(node3)
	if q.Head == q.Tail {
		t.Errorf("Expected queue sentinel nodes to not be equal but got head as %+v and tail as %+v", q.Head, q.Tail)
	}

	if q.Size != 0 {
		t.Errorf("Expected queue size to be 0 but got %d", q.Size)
	}

}

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
