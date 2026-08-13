from prefect import task, flow, get_run_logger
import time
import os

# ============================================================
# 主題二：Docker Worker
# ============================================================
#
# 架構：
#   本機 Prefect Server（:4200）
#         ↕ API
#   Docker Work Pool
#         ↕ 排程觸發
#   Docker Worker（本機監聽）
#         ↕ 啟動容器
#   Docker Container（執行 Flow，跑完自動銷毀）
#
# 與主題一（Process Worker）差異：
#   Process → Flow 直接在本機 Process 執行
#   Docker  → Flow 在全新容器內執行，跑完容器消失
# ============================================================


# ── Tasks ──────────────────────────────────────────────────

@task(name="環境資訊確認")
def check_env():
    logger = get_run_logger()
    logger.info(f"🐳 Container hostname：{os.environ.get('HOSTNAME', 'unknown')}")
    logger.info(f"📡 Prefect API：{os.environ.get('PREFECT_API_URL', 'not set')}")


@task(name="資料擷取")
def extract_data():
    logger = get_run_logger()
    logger.info("🔍 [容器內] 正在擷取資料...")
    time.sleep(1)
    return {"records": 200, "source": "Docker Container"}


@task(name="資料轉換")
def transform_data(raw: dict):
    logger = get_run_logger()
    logger.info(f"⚙️  [容器內] 正在轉換 {raw['records']} 筆資料...")
    time.sleep(1)
    return {"records": raw["records"], "status": "transformed"}


@task(name="資料載入")
def load_data(data: dict):
    logger = get_run_logger()
    logger.info(f"💾 [容器內] 正在載入 {data['records']} 筆資料...")
    time.sleep(1)
    logger.info("✅ 容器內 ETL 完成，容器即將釋放！")


# ── Flow ────────────────────────────────────────────────────

@flow(name="ETL Pipeline（Docker 版）")
def etl_pipeline_docker():
    check_env()
    raw     = extract_data()
    cleaned = transform_data(raw)
    load_data(cleaned)


# ── Deploy ──────────────────────────────────────────────────

if __name__ == "__main__":
    # 步驟 1：Build Image（在專案根目錄執行，只需一次）
    #   docker build -f docker/Dockerfile -t prefect-demo:latest .
    #
    # 步驟 2：建立 Docker Work Pool（只需一次）
    #   prefect work-pool create docker-pool --type docker
    #
    # 步驟 3：啟動 Docker Worker（開新 terminal，需持續跑著）
    #   PREFECT_API_URL=http://127.0.0.1:4200/api uv run prefect worker start --pool docker-pool
    #
    # 步驟 4：執行此檔案，登記 Deployment
    #   PREFECT_API_URL=http://127.0.0.1:4200/api uv run ./docker/deploy_docker.py
    #
    # 步驟 5：到 UI 觸發 Run → 觀察容器被啟動！
    #   http://127.0.0.1:4200

    etl_pipeline_docker.deploy(
        name="etl-docker-deployment",
        work_pool_name="docker-pool",

        # 指定要使用的 Docker Image（先 build 好）
        image="prefect-demo:latest",
        push=False,  # 不推到 Registry

        # ── 關鍵：告訴 Worker 不要去 Docker Hub pull，直接用本機 image ──
        # Never     → 永遠用本機，找不到就報錯（本機開發用）
        # IfNotPresent → 本機沒有才 pull（正式環境推薦）
        # Always    → 每次都重新 pull（CI/CD 用）
        job_variables={"image_pull_policy": "Never"},

        # ── 可選：Cron 排程 ──────────────────────────────────
        # from prefect.schedules import Cron
        # schedules=[Cron("0 8 * * *", timezone="Asia/Taipei")],
    )
