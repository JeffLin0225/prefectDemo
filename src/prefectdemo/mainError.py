import random
from prefect import task, flow, get_run_logger

# 設定重試 3 次，每次間隔 2 秒
@task(name="模擬不穩定的 API 連線", retries=3, retry_delay_seconds=2)
def unstable_request():
    logger = get_run_logger()
    logger.info("🌐 正在連線至外部 API...")
    if random.choice([True, False]):  # 50% 機率失敗
        logger.warning("❌ 連線失敗！準備重試...")
        raise ValueError("網路連線超時！")
    logger.info("✅ 連線成功！")
    return "Data Payload"

@flow(name="測試自動重試機制")
def retry_demo_flow():
    unstable_request()

if __name__ == "__main__":
    retry_demo_flow()