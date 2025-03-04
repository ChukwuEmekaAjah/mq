package mq

import (
	"time"

	"github.com/ChukwuEmekaAjah/mq/internal/models"
)

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

// Message represents a message sent by a client
type Message struct {
	InsertedAt                  time.Time
	ReadAt                      time.Time
	MessageVisibilityTimesOutAt time.Time

	MessageID               string                             `json:"messageId"`
	Body                    string                             `json:"body"`
	MD5OfBody               string                             `json:"md5OfBody"`
	Attributes              Attributes                         `json:"attributes"`
	MessageAttributes       map[string]models.MessageAttribute `json:"messageAttributes"`
	MessageSystemAttributes map[string]models.MessageAttribute
	MD5OfMessageAttributes  string `json:"md5OfMessageAttributes"`
	ReceiptHandle           string `json:"receiptHandle"`
}
