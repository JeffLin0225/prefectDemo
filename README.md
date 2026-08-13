# Prefect Demo 教學專案

## 環境初始化

```bash
# 建立專案
uv init

# 加入 prefect 套件
uv add prefect

# 啟動 Prefect Server（Dashboard）
uv run prefect server start
```

Dashboard 網址：http://127.0.0.1:4200

---

## 目錄結構

```
prefectDemo/
├── src/prefectdemo/
│   ├── main.py          # 基礎 Flow / Task / Logger / Retry
│   ├── mainError.py     # 模擬隨機失敗 + 自動重試
│   └── cycle.py         # submit() 並行任務 + 子 Flow
├── cron/
│   └── cycle.py         # Cron 排程 + .serve()
├── worker/
│   └── deploy.py        # Work Pool + .deploy() 部署
├── docker/              # (TODO) Docker Worker
└── serverless/          # (TODO) Serverless 本機模擬
```

---

## 主題一：基礎 Flow / Task

```bash
# 執行基礎範例
uv run src/prefectdemo/main.py

# 執行錯誤重試範例
uv run src/prefectdemo/mainError.py

# 執行並行任務範例
uv run src/prefectdemo/cycle.py
```

---

## 主題二：Cron 排程（.serve）

> `.serve()` 腳本本身就是 Worker，需要持續跑著才能接排程。

```bash
# 連到已啟動的 Prefect Server 並啟動排程
PREFECT_API_URL=http://127.0.0.1:4200/api uv run ./cron/cycle.py
```

---

## 主題三：Work Pool & Worker（.deploy）

> `.deploy()` 將 Flow「登記」到 Work Pool，由獨立的 Worker 執行。

### .serve() vs .deploy() 差異

| | `.serve()` | `.deploy()` |
|---|---|---|
| **角色** | 腳本自己是 Worker | 只做「登記」，Worker 獨立啟動 |
| **適合** | 本機快速開發 | 正式環境、多 Worker、換執行環境 |
| **執行環境** | 固定為本機 Process | 可換成 Docker / Cloud Run |

### 操作步驟

```bash
# 步驟 1：建立 Work Pool（只需執行一次）
prefect work-pool create my-pool --type process

# 步驟 2：啟動 Worker（開新 terminal，需持續跑著）
PREFECT_API_URL=http://127.0.0.1:4200/api uv run prefect worker start --pool my-pool

# 步驟 3：部署 Flow 到 Work Pool（登記）
PREFECT_API_URL=http://127.0.0.1:4200/api uv run ./worker/deploy.py

# 步驟 4：在 UI 手動觸發 Run，或等排程自動執行
# http://127.0.0.1:4200
```

---

## 主題四：Docker Worker（Container）

> 每次觸發都啟動一個全新容器，執行完自動銷毀。環境完全隔離。

### Process vs Docker 差異

| | Process Worker | Docker Worker |
|---|---|---|
| **執行環境** | 直接在本機 Process | 每次啟動全新 Container |
| **環境隔離** | 與主機共用 Python 環境 | 完全隔離（Image 固定） |
| **接近生產** | ❌ | ✅（Cloud Run / ECS 也是容器）|
| **冷啟動** | 快 | 較慢（需啟動容器）|

### 操作步驟

# 加入 prefect-docker 套件
uv add prefect-docker

```bash
# 步驟 1：Build Docker Image（在專案根目錄執行）
docker build -f docker/Dockerfile -t prefect-demo:latest .

# 步驟 2：建立 Docker Work Pool（只需一次）
prefect work-pool create docker-pool --type docker

# 步驟 3：啟動 Docker Worker（開新 terminal，需持續跑著）
PREFECT_API_URL=http://127.0.0.1:4200/api uv run prefect worker start --pool docker-pool

# 步驟 4：登記 Deployment
PREFECT_API_URL=http://127.0.0.1:4200/api uv run ./docker/deploy_docker.py

# 步驟 5：觸發執行，觀察容器被啟動
prefect deployment run 'ETL Pipeline（Docker 版）/etl-docker-deployment'
```

### 為什麼 Image 裡要設定 `PREFECT_API_URL`？

容器是獨立的網路環境，無法直接用 `localhost:4200` 連回主機。  
`host.docker.internal` 是 OrbStack / Docker Desktop 提供的特殊 hostname，指向主機 IP。

```
[Docker Container]
    → host.docker.internal:4200
        → [主機] Prefect Server :4200
```

---

## 常用指令速查

```bash
# ── Work Pool ──────────────────────────────────────────────
prefect work-pool ls
prefect work-pool create my-pool --type process
prefect work-pool create docker-pool --type docker
prefect work-pool delete my-pool

# ── Deployment ────────────────────────────────────────────
prefect deployment ls
prefect deployment run 'ETL Pipeline（Work Pool 版）/etl-deployment'
prefect deployment run 'ETL Pipeline（Docker 版）/etl-docker-deployment'

# ── Docker ────────────────────────────────────────────────
docker build -f docker/Dockerfile -t prefect-demo:latest .
docker images | grep prefect-demo
```
