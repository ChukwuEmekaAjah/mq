package models

// DeleteMessageBatchRequestEntry is an item in the array of messages to be deleted from queue
type DeleteMessageBatchRequestEntry struct {
	ID            string `json:"Id"`
	ReceiptHandle string
}
