import random
from prefect import task, flow, get_run_logger

@task
def fetch_user_ids():
    logger = get_run_logger()
    logger.info("📋 正在取得使用者清單...")
    return [101, 102, 103]

@task
def send_mail(user_id):
    logger = get_run_logger()
    if user_id == 102:
        raise ValueError(f"⚠️ User {user_id} 無效，跳過寄送")
        
    logger.info(f"✉️ Mail sent to user {user_id}")

@flow(name="動態依賴＆關卡測試")
def email_pipeline():
    logger = get_run_logger()
    user_ids = fetch_user_ids()  # 新版 Prefect 在 flow 內直接回傳真實值

    # 用 submit() 讓每個 task 獨立執行，失敗不影響其他
    results = []
    for uid in user_ids:
        future = send_mail.submit(uid)
        results.append(future)

    # # 等全部完成，忽略個別失敗
    # for f in results:
    #     try:
    #         f.result()
    #     except Exception as e:
    #         logger.warning(f"⚠️ Task 失敗被捕捉: {e}")

    logger.info("✅ 全部寄送流程完畢!")

@flow(name="總控流程")
def main_pipeline():
    email_pipeline()


if __name__ == "__main__":
    main_pipeline()