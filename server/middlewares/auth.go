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

package middlewares

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/auula/urnadb/clog"
	"github.com/auula/urnadb/server/response"
	"github.com/gin-gonic/gin"
)

var (
	authPassword string
	allowIpList  []string
)

func SetAuthPassword(password string) {
	authPassword = password
}

func SetAllowIpList(ipList []string) {
	allowIpList = ipList
}

func AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 从请求头中获取 "Auth-Token" 字段的值
		auth := c.GetHeader("Auth-Token")
		clog.Debugf("HTTP request header authorization: %v", c.Request)

		// 获取客户端 IP 地址
		ip := c.GetHeader("X-Forwarded-For")
		if ip == "" {
			ip = c.ClientIP()
		}

		// 检查 IP 白名单
		if len(allowIpList) > 0 {
			ok := false
			for _, allowedIP := range allowIpList {
				// 只要找到匹配的 IP，就终止循环
				if allowedIP == strings.Split(ip, ":")[0] {
					ok = true
					break
				}
			}
			if !ok {
				clog.Warnf("Unauthorized IP address: %s", ip)
				c.IndentedJSON(
					http.StatusUnauthorized,
					response.FailJSON(fmt.Sprintf("client IP %s is not allowed!", ip)))
				c.Abort()
				return
			}
		}

		if auth != authPassword {
			clog.Warnf("Unauthorized access attempt from client %s", ip)
			c.IndentedJSON(http.StatusUnauthorized, response.FailJSON("access not authorised!"))
			c.Abort()
			return
		}

		// 如果验证通过，继续执行后续的处理程序
		c.Next()
	}
}
