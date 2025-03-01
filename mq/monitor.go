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

			for head := queueDB.(*PriorityQueue).Front(); head != nil; head = queueDB.(*PriorityQueue).Front() {
				if head.val.MessageVisibilityTimesOutAt.Before(time.Now()) {
					item, _ := queueDB.(*PriorityQueue).Dequeue()
					receivedMessagesMapDB.(*sync.Map).Delete(item.val.ReceiptHandle)

					messagesDB.(*Queue).Enqueue(&QueueNode{
						Val: item.val,
					})
					item = nil
				} else {
					// if the front message is not expired, other messages behind it won't be expired
					break
				}
			}
			return true
		})
		time.Sleep(time.Second * time.Duration(10))
	}
}
