from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact, create_table_artifact
import time

@task(name="處理訂單數據")
def process_orders():
    logger = get_run_logger()
    logger.info("📦 正在統計今日訂單與營收...")
    time.sleep(1)
    
    # 模擬產出的統計結果
    summary = {
        "total_orders": 1250,
        "success_orders": 1248,
        "failed_orders": 2,
        "total_revenue": 358900
    }
    return summary

@flow(name="每日營收結算（含 UI 報表）")
def daily_revenue_flow():
    stats = process_orders()

    # ── 1. 產出 Markdown 報表 ─────────────────────────────────
    report_md = f"""
# 📊 每日營收結算報告
* **結算時間**：`2026-09-07`
* **總訂單數**：`{stats['total_orders']} 筆`
* **成功率**：`{(stats['success_orders'] / stats['total_orders']) * 100:.2f}%`
* **總營業額**：`NT$ {stats['total_revenue']:,}`

---
> ⚠️ **異常注意**：今日有 **{stats['failed_orders']}** 筆訂單付款超時，已轉入人工稽核隊列。
"""
    create_markdown_artifact(
        key="daily-revenue-report",
        markdown=report_md,
        description="每日營收統計摘要"
    )

    # ── 2. 產出 Table 表格 ───────────────────────────────────
    table_data = [
        {"門市": "台北信義店", "訂單數": 520, "狀態": "正常"},
        {"門市": "台中中港店", "訂單數": 430, "狀態": "正常"},
        {"門市": "高雄巨蛋店", "訂單數": 300, "狀態": "異常 (2筆失敗)"},
    ]
    create_table_artifact(
        key="store-breakdown",
        table=table_data,
        description="各門市營運細項"
    )

if __name__ == "__main__":
    daily_revenue_flow()
