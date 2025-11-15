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

package routes

import (
	"github.com/auula/urnadb/server/controllers"
	"github.com/auula/urnadb/server/middlewares"
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
	router.Use(middlewares.AuthMiddleware())

	// 404处理
	router.NoRoute(controllers.Error404Handler)
	router.NoMethod(controllers.Error404Handler)

	// 健康检查
	router.GET("/", controllers.GetHealthController)

	// 查询路由
	query := router.Group("/query")
	{
		query.GET("/:key", controllers.QueryController)
	}

	// Table 路由
	tables := router.Group("/tables")
	{
		tables.GET("/:key", controllers.QueryTableController)
		tables.PUT("/:key", controllers.CreateTableController)
		tables.DELETE("/:key", controllers.DeleteTableController)
		tables.PATCH("/:key", controllers.PatchRowsTableController)
		tables.GET("/:key/rows", controllers.QueryRowsTableController)
		tables.POST("/:key/rows", controllers.InsertRowsTableController)
		tables.DELETE("/:key/rows", controllers.RemoveRowsTabelController)
	}

	// Lock 路由
	locks := router.Group("/locks")
	{
		locks.PUT("/:key", controllers.NewLockController)
		locks.PATCH("/:key", controllers.DoLeaseLockController)
		locks.DELETE("/:key", controllers.DeleteLockController)
	}

	// records 路由
	records := router.Group("/records")
	{
		records.GET("/:key", controllers.GetRecordsController)
		records.PUT("/:key", controllers.PutRecordsController)
		records.POST("/:key", controllers.SearchRecordsController)
		records.DELETE("/:key", controllers.DeleteRecordsController)
	}

	// Variants 路由
	variants := router.Group("/variants")
	{
		variants.GET("/:key", controllers.GetVariantController)
		variants.POST("/:key", controllers.MathVariantController)
		variants.PUT("/:key", controllers.CreateVariantController)
		variants.DELETE("/:key", controllers.DeleteVariantController)
	}

	return router
}
