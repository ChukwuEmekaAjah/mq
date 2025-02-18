package models

// DeleteMessageBatchResult is a collection of both failed and successful batch delete request results
type DeleteMessageBatchResult struct {
	Failed     []BatchResultErrorEntry
	Successful []DeleteMessageBatchResultEntry
}
