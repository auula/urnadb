package response

type ResponseEntity struct {
	Status  string      `json:"status"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

// 返回成功响应
func Ok(data interface{}) *ResponseEntity {
	return &ResponseEntity{
		Status: "success",
		Data:   data,
	}
}

// 返回失败响应
func Fail(message string) *ResponseEntity {
	return &ResponseEntity{
		Status:  "error",
		Message: message,
	}
}
