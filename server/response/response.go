// Copyright 2022 Leon Ding <ding_ms@outlook.com> https://urnadb.github.io

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package response

type ResponseEntity struct {
	Status  string      `json:"status"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

// 返回成功响应
func Ok(message string, data interface{}) *ResponseEntity {
	return &ResponseEntity{
		Status:  "success",
		Message: message,
		Data:    data,
	}
}

// 返回失败响应
func Fail(message string) *ResponseEntity {
	return &ResponseEntity{
		Status:  "error",
		Message: message,
		Data:    nil,
	}
}
