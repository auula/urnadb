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

package router

import (
	"github.com/auula/urnadb/server/controller"
	"github.com/auula/urnadb/server/middleware"
	"github.com/gin-gonic/gin"
)

const version = "urnadb/1.5.0"

func SetupRoutes() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()

	// 全局中间件：添加 Server 响应头，这里加上服务器的版本号
	router.Use(func(c *gin.Context) {
		c.Header("Server", version)
		c.Next()
	})

	// 全局中间件
	router.Use(middleware.AuthMiddleware())

	// 404 处理
	router.NoRoute(controller.Error404Handler)
	router.NoMethod(controller.Error404Handler)

	// 健康检查
	router.GET("/health", controller.HealthController)

	// 查询路由
	query := router.Group("/query")
	{
		query.GET("/:key", controller.QueryController)
	}

	// Table 路由
	tables := router.Group("/tables")
	{
		tables.GET("/:key", controller.QueryTableController)
		tables.PUT("/:key", controller.CreateTableController)
		tables.DELETE("/:key", controller.DeleteTableController)
		tables.PATCH("/:key", controller.PatchRowsTableController)
		tables.GET("/:key/rows", controller.QueryRowsTableController)
		tables.POST("/:key/rows", controller.InsertRowsTableController)
		tables.DELETE("/:key/rows", controller.RemoveRowsTabelController)
	}

	// Lock 路由
	locks := router.Group("/locks")
	{
		locks.PUT("/:key", controller.NewLockController)
		locks.PATCH("/:key", controller.DoLeaseLockController)
		locks.DELETE("/:key", controller.DeleteLockController)
	}

	// Record 路由
	records := router.Group("/records")
	{
		records.GET("/:key", controller.GetRecordController)
		records.PUT("/:key", controller.PutRecordController)
		records.POST("/:key", controller.SearchRecordController)
		records.DELETE("/:key", controller.DeleteRecordController)
	}

	// Variant 路由
	variants := router.Group("/variants")
	{
		variants.GET("/:key", controller.GetVariantController)
		variants.POST("/:key", controller.MathVariantController)
		variants.PUT("/:key", controller.CreateVariantController)
		variants.DELETE("/:key", controller.DeleteVariantController)
	}

	return router
}
