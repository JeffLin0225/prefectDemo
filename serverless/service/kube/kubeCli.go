package kube

import (
	"prefect-serverless/config"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type KubeCli struct {
	client *kubernetes.Clientset
	cfg    *config.Config
}

func NewKubeCli(config *config.Config) (*KubeCli, error) {

	// 1. rest.InClusterConfig()：
	// 專門用於「程式本身就跑在 K8s Pod 內部」的情境。
	// 它會自動去讀取 K8s 掛載在 Pod 內部的 ServiceAccount Token 與 CA 憑證
	// （路徑通常在 /var/run/secrets/kubernetes.io/serviceaccount/），
	// 並自動取得叢集 API Server 的內部連線位址，生成連線設定物件 (rest.Config)。
	// ※ 注意：如果在 Mac/Windows 本機直接執行此程式，因為沒有這些 Pod 內部檔案，這一步會報錯。
	k8sConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	// 2. kubernetes.NewForConfig(k8sConfig)：
	// 根據第一步產生的連線設定 (k8sConfig)，正式建立一個 Clientset 實例。
	// Clientset 包含了所有與 K8s API Server 溝通的 REST 客戶端，
	// 後續所有建立 Pod、Job、Deployment 的 API 呼叫都是透過這個實例完成。
	clientSet, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return nil, err
	}

	return &KubeCli{
		client: clientSet,
		cfg:    config,
	}, nil
}
