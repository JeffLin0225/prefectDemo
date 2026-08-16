package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

// ============================================================
// 主題三：Serverless 本機模擬
// ============================================================
//
// 架構：
//   [Client] --POST /run--> [Go HTTP Server（本機）]
//                                   ↓ client-go
//                            [K8s Job 建立]
//                                   ↓
//                            [K8s Pod 啟動]（prefect-demo image）
//                                   ↓
//                            執行 serverless/flow.py
//                                   ↓
//                            Pod 完成 → 自動銷毀（TTL 120s）
//
// Serverless 核心精神：
//   - 無狀態：每次 Job 都是全新 Pod
//   - 按需啟動：沒有請求就沒有 Pod
//   - 執行完釋放：TTL 到期自動清理
// ============================================================

var (
	clientset     *kubernetes.Clientset
	prefectAPIURL = "http://host.docker.internal:4200/api"
	namespace     = "default"
	image         = "prefect-demo:latest"
)

func init() {
	if url := os.Getenv("PREFECT_API_URL"); url != "" {
		prefectAPIURL = url
	}
}

func main() {
	// ── 初始化 K8s Client（讀取 OrbStack 的 kubeconfig）────
	kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Fatalf("❌ 無法載入 kubeconfig: %v", err)
	}

	clientset, err = kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("❌ 無法建立 K8s client: %v", err)
	}

	// ── 路由設定 ────────────────────────────────────────────
	mux := http.NewServeMux()
	mux.HandleFunc("POST /run", runHandler)
	mux.HandleFunc("GET /health", healthHandler)

	log.Println("🚀 Serverless Trigger Server 啟動在 :8080")
	log.Println("   POST /run    → 觸發 K8s Job，啟動 Prefect 批次")
	log.Println("   GET  /health → 健康檢查")
	log.Fatal(http.ListenAndServe(":8080", mux))
}

// ── Request / Response 結構 ─────────────────────────────────

type RunRequest struct {
	JobSuffix string `json:"job_suffix,omitempty"` // 可選，預設用 timestamp
}

type RunResponse struct {
	Status    string `json:"status"`
	JobName   string `json:"job_name"`
	Namespace string `json:"namespace"`
	Message   string `json:"message"`
}

// ── Handlers ────────────────────────────────────────────────

func runHandler(w http.ResponseWriter, r *http.Request) {
	var req RunRequest
	_ = json.NewDecoder(r.Body).Decode(&req)

	// 產生唯一的 Job 名稱
	suffix := req.JobSuffix
	if suffix == "" {
		suffix = fmt.Sprintf("%d", time.Now().Unix())
	}
	jobName := fmt.Sprintf("prefect-batch-%s", suffix)

	log.Printf("📦 建立 K8s Job: %s", jobName)

	job := buildJob(jobName)
	_, err := clientset.BatchV1().Jobs(namespace).Create(
		context.Background(), job, metav1.CreateOptions{},
	)
	if err != nil {
		log.Printf("❌ Job 建立失敗: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	log.Printf("✅ Job 建立成功: %s → Pod 即將啟動", jobName)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(RunResponse{
		Status:    "triggered",
		JobName:   jobName,
		Namespace: namespace,
		Message:   fmt.Sprintf("K8s Job '%s' 已建立，Pod 即將啟動執行批次", jobName),
	})
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// ── K8s Job 建構 ────────────────────────────────────────────

func buildJob(name string) *batchv1.Job {
	backoffLimit := int32(0)  // 失敗不重試（Serverless：失敗就丟）
	ttl := int32(120)         // Job 完成後 120 秒自動清理 Pod

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app":        "prefect-serverless",
				"managed-by": "go-trigger",
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl, // 完成後自動清理
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": "prefect-batch-runner",
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:  "batch-runner",
							Image: image,
							// 永遠使用本機 image（不去 Docker Hub pull）
							ImagePullPolicy: corev1.PullNever,
							// 執行 Prefect Flow
							Command: []string{
								"uv", "run", "python", "serverless/flow.py",
							},
							Env: []corev1.EnvVar{
								{
									// 讓 Pod 內的 Prefect 能連回主機的 Server
									Name:  "PREFECT_API_URL",
									Value: prefectAPIURL,
								},
							},
						},
					},
				},
			},
		},
	}
}
