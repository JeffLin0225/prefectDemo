package router

import (
	"prefect-serverless/config"
	"prefect-serverless/controller"
	"prefect-serverless/service/kube"

	"github.com/gin-gonic/gin"
)

func SetupRouter(cfg *config.Config, kubeCli *kube.KubeCli) *gin.Engine {

	// 初始化 Gin 引擎
	r := gin.Default()

	runCtrl := controller.NewRunController(kubeCli)

	// 註冊路由群組
	r.GET("/health", controller.HealthCheck)

	api := r.Group("/api")
	{
		api.POST("run", runCtrl.HandleRun)
	}

	return r
}
