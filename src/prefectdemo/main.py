from prefect import task, flow, get_run_logger
import time

@task(name="檢查系統環境", retries=2, retry_delay_seconds=1)
def check_environment():
    logger = get_run_logger()
    logger.info("檢查 python 環境中...")
    time.sleep(1)
    return "Env OK!!!"

@task(name="執行資料處理")
def process_data(status):
    logger = get_run_logger()
    logger.info(f"正在處理 Prefect 任務，當前狀態為：{status}")
    time.sleep(1)
    logger.info("Prefect 任務完成")

@flow(name="我的第一個 uv + prefact 工作流")
def my_first_flow():
    env_status = check_environment()
    process_data(env_status)

if __name__ == "__main__":
    my_first_flow()