package mq

import (
	"time"
)

// MaxMessageVisibilityTimeout specifies how long a message can be processed by a consumer
const MaxMessageVisibilityTimeout int = 1 * 60

// Monitor looks at the store DB to remove messages that have exceeded their visibility timeout
func Monitor(store *Store) {
	for {
		store.Monitor()
		time.Sleep(time.Second * time.Duration(10))
	}
}
