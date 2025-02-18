package models

// BatchResultErrorEntry represents a response from a failed batch request action
type BatchResultErrorEntry struct {
	Code        string
	ID          string `json:"Id"`
	Message     string
	SenderFault bool
}
