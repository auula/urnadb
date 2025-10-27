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
		table.GET("/:key", controllers.GetTableController)
		table.PUT("/:key", controllers.PutTableController)
		table.DELETE("/:key", controllers.DeleteTableController)
	}

	// Lock 路由
	lock := router.Group("/locks")
	{
		lock.PUT("/:key", controllers.NewLeaseController)
	}

	// // records 路由
	records := router.Group("/records")
	{
		records.GET("/:key", controllers.GetRecordsController)
		records.PUT("/:key", controllers.PutRecordsController)
		records.DELETE("/:key", controllers.DeleteRecordsController)
	}

	// // Set 路由
	// set := router.Group("/sets")
	// {
	// 	set.GET("/:key", controllers.GetSetController)
	// 	set.PUT("/:key", controllers.PutSetController)
	// 	set.DELETE("/:key", controllers.DeleteSetController)
	// }

	// // ZSet 路由
	// zset := router.Group("/zsets")
	// {
	// 	zset.GET("/:key", controllers.GetZsetController)
	// 	zset.PUT("/:key", controllers.PutZsetController)
	// 	zset.DELETE("/:key", controllers.DeleteZsetController)
	// }

	// // Collection 路由
	// collection := router.Group("/collections")
	// {
	// 	collection.GET("/:key", controllers.GetCollectionController)
	// 	collection.PUT("/:key", controllers.PutCollectionController)
	// 	collection.DELETE("/:key", controllers.DeleteCollectionController)
	// }

	return router
}
