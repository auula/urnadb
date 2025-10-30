package routes

import (
	"github.com/auula/urnadb/server/controllers"
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
	table := router.Group("/tables")
	{
		table.GET("/:key", controllers.QueryTableController)
		table.PUT("/:key", controllers.CreateTableController)
		table.POST("/:key", controllers.PatchRowsTableController)
		table.DELETE("/:key", controllers.DeleteTableController)
	}

	// Lock 路由
	lock := router.Group("/locks")
	{
		lock.PUT("/:key", controllers.NewLockController)
		lock.PATCH("/:key", controllers.DoLeaseLockController)
		lock.DELETE("/:key", controllers.DeleteLockController)
	}

	// // records 路由
	records := router.Group("/records")
	{
		records.GET("/:key", controllers.GetRecordsController)
		records.PUT("/:key", controllers.PutRecordsController)
		records.DELETE("/:key", controllers.DeleteRecordsController)
	}

	return router
}
