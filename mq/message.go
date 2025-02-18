package mq

import "time"

// Attributes represents the attributes of a message
type Attributes struct {
	ApproximateReceiveCount          uint
	ApproximateFirstReceiveTimeStamp int
	MessageDeduplicationID           string
	MessageGroupID                   string
	SenderID                         string
	SentTimestamp                    int64
	SequenceNumber                   uint
}

// MessageAttributeValue represents the actual value that
// a user-specified message attribute has
type MessageAttributeValue interface{}

// MessageAttribute represents a user-specified message attribute
type MessageAttribute struct {
	Type  string
	Value MessageAttributeValue
}

// Message represents a message sent by a client
type Message struct {
	InsertedAt time.Time
	ReadAt     time.Time

	MessageID              string                      `json:"messageId"`
	Body                   string                      `json:"body"`
	MD5OfBody              string                      `json:"md5OfBody"`
	Attributes             Attributes                  `json:"attributes"`
	MessageAttributes      map[string]MessageAttribute `json:"messageAttributes"`
	MD5OfMessageAttributes string                      `json:"md5OfMessageAttributes"`
	ReceiptHandle          string                      `json:"receiptHandle"`
}
