package models

// BatchResult is a collection of both failed and successful batch delete request results
type BatchResult struct {
	Failed     []BatchResultErrorEntry
	Successful []map[string]string
}
