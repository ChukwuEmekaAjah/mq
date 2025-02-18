package mq

import (
	"sync"
	"time"
)

// MaxMessageVisibilityTimeout specifies how long a message can be processed by a consumer
const MaxMessageVisibilityTimeout int = 1 * 60

// Monitor looks at the store DB to remove messages that have exceeded their visibility timeout
func Monitor(store *Store) {
	for {
		store.receivedMessagesQueues.Range(func(queue, queueDB interface{}) bool {
			receivedMessagesMapDB, _ := store.receivedMessagesMap.Load(queue.(string))
			messagesDB, _ := store.queues.Load(queue.(string))

			for head := queueDB.(*DLLQueue).Front(); head != nil; head = queueDB.(*DLLQueue).Front() {
				if head.Val.ReadAt.Add(time.Second * time.Duration(MaxMessageVisibilityTimeout)).Before(time.Now()) {
					message, _ := queueDB.(*DLLQueue).Dequeue()
					receivedMessagesMapDB.(*sync.Map).Delete(message.Val.ReceiptHandle)

					messagesDB.(*Queue).Enqueue(&QueueNode{
						Val: message.Val,
					})
				} else {
					// if the front message is not expired, other messages behind it won't be expired
					break
				}
			}
			return true
		})
		time.Sleep(time.Second)
	}
}
