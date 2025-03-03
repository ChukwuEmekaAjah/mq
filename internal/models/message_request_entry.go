package models

// MessageAttributeValue represents the actual value that
// a user-specified message attribute has
type MessageAttributeValue interface{}

// MessageAttribute represents a user-specified message attribute
type MessageAttribute struct {
	Type  string
	Value MessageAttributeValue
}

// MessageRequest represents a single message sent by a client to the server
type MessageRequest struct {
	ID                      string `json:"Id"`
	DelaySeconds            int
	MessageAttributes       map[string]MessageAttribute
	MessageBody             string
	MessageDeduplicationID  string
	MessageGroupID          string
	MessageSystemAttributes map[string]MessageAttribute
}
