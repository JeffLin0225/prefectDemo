from prefect import task, flow, get_run_logger
import time

# ============================================================
# 主題一：Work Pool & Worker
# ============================================================
#
# 核心概念：
#   .serve()  → 腳本「自己」就是 Worker，需要持續跑著才能接排程
#   .deploy() → 只是把 Flow「登記」到 Work Pool，
#               實際執行由獨立啟動的 Worker process 負責
#
# 這樣的好處：
#   1. Flow 定義 與 執行環境 解耦
#   2. 可以有多個 Worker 同時消化同一個 Pool 的任務
#   3. 換執行環境（Process → Docker → Cloud Run）只需改 Work Pool
# ============================================================


# ── Tasks ──────────────────────────────────────────────────

@task(name="資料擷取")
def extract_data():
    logger = get_run_logger()
    logger.info("🔍 正在擷取資料來源...")
    time.sleep(1)
    return {"records": 100, "source": "PostgreSQL"}


@task(name="資料轉換")
def transform_data(raw: dict):
    logger = get_run_logger()
    logger.info(f"⚙️  正在轉換 {raw['records']} 筆資料...")
    time.sleep(1)
    return {"records": raw["records"], "status": "transformed"}


@task(name="資料載入")
def load_data(data: dict):
    logger = get_run_logger()
    logger.info(f"💾 正在載入 {data['records']} 筆資料至目標...")
    time.sleep(1)
    logger.info("✅ ETL 完成！")


# ── Flow ────────────────────────────────────────────────────

@flow(name="ETL Pipeline（Work Pool 版）")
def etl_pipeline():
    """
    標準 ETL 三步驟：Extract → Transform → Load
    透過 Work Pool 執行，與排程及執行環境完全解耦。
    """
    raw     = extract_data()
    cleaned = transform_data(raw)
    load_data(cleaned)


# ── Deploy ──────────────────────────────────────────────────

if __name__ == "__main__":
    # ── 為什麼要用 from_source()？ ────────────────────────────
    #
    # Prefect 3 的 .deploy() 需要知道 Worker 要去哪裡「取得程式碼」
    # 可以是：
    #   - Git Repo（雲端）→ 給 CI/CD 用
    #   - Docker Image   → 主題二會用
    #   - 本機路徑       → from_source("/path") ← 現在用這個
    #
    # 步驟說明：
    # 1. 建立 Work Pool（只需執行一次）：
    #    prefect work-pool create my-pool --type process
    #
    # 2. 啟動 Worker（需持續跑著，開新 terminal）：
    #    PREFECT_API_URL=http://127.0.0.1:4200/api uv run prefect worker start --pool my-pool
    #
    # 3. 執行此檔案，將 Flow 登記到 Work Pool：
    #    PREFECT_API_URL=http://127.0.0.1:4200/api uv run ./worker/deploy.py
    #
    # 4. 到 UI 手動觸發：http://127.0.0.1:4200

    etl_pipeline.from_source(
        # 告訴 Worker：程式碼在本機的這個目錄底下
        source="/Users/jeff/Desktop/prefectDemo",

        # 入口點格式：「相對路徑:flow函式名稱」
        entrypoint="worker/deploy.py:etl_pipeline",
    ).deploy(
        name="etl-deployment",          # Deployment 名稱（顯示在 UI 上）
        work_pool_name="my-pool",        # 對應步驟 1 建立的 Work Pool

        # ── 可選：加上 Cron 排程 ──────────────────────────────
        # from prefect.schedules import Cron
        # schedules=[Cron("*/5 * * * *", timezone="Asia/Taipei")],

        # ── 可選：加上標籤，方便在 UI 過濾 ───────────────────
        # tags=["etl", "daily"],
    )
