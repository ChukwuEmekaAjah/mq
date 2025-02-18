package models

// ChangeMessageVisibilityRequest specifies request body for
// updating the visibility timeout of a message
type ChangeMessageVisibilityRequest struct {
	ReceiptHandle     string
	VisibilityTimeout int
}
