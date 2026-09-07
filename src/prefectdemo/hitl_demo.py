from prefect import flow, task, get_run_logger
from prefect.flow_runs import pause_flow_run
from prefect.input import RunInput
from pydantic import Field
import time

# ── 定義 UI 彈出的表單欄位 ──────────────────────────────────
class ApprovalForm(RunInput):
    approver: str = Field(description="審核人姓名")
    approved: bool = Field(default=False, description="是否同意發布到生產資料庫？")
    reason: str = Field(default="", description="備註 / 審核原因")

@task(name="資料清洗與驗證")
def prepare_data():
    logger = get_run_logger()
    logger.info("🔍 正在檢查資料庫異動筆數...")
    time.sleep(1)
    return {"affected_rows": 50000}

@task(name="寫入生產資料庫")
def write_to_production(approver: str, reason: str):
    logger = get_run_logger()
    logger.info(f"🚀 [已獲核准] 審核人: {approver}，原因: {reason}")
    logger.info("💾 正式寫入 50,000 筆資料至 Production DB...")
    time.sleep(1)
    logger.info("✅ 發布完成！")

@flow(name="生產資料庫同步（含人工審批）")
def sync_production_flow():
    logger = get_run_logger()
    data = prepare_data()
    
    logger.warning(f"⚠️ 偵測到異動資料量過大（{data['affected_rows']} 筆），觸發人工審批安全門檻！")

    # ── 流程在此自動「暫停」，等待網頁端輸入 ─────────────────────
    user_feedback = pause_flow_run(
        wait_for_input=ApprovalForm
    )

    # ── 接收到網頁傳回的輸入，繼續判斷後續邏輯 ───────────────────
    if user_feedback.approved:
        write_to_production(user_feedback.approver, user_feedback.reason)
    else:
        logger.error(f"❌ 流程遭 {user_feedback.approver} 駁回！原因：{user_feedback.reason}")
        raise RuntimeError("發布流程被人工中止！")

if __name__ == "__main__":
    sync_production_flow()
