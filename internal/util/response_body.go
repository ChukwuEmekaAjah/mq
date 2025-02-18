package util

// ResponseBody is a representation of an HTTP response
type ResponseBody struct {
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}
