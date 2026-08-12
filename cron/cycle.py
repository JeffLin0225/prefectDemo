import random
from prefect import task, flow, get_run_logger
from prefect.schedules import Cron

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
    
    # 本機部署用 serve()，需持續跑著才能自動執行 cron
    # 連到已在跑的 server：PREFECT_API_URL=http://127.0.0.1:4200/api uv run ./cron/cycle.py
    main_pipeline.serve(

        name="first_cron_Job",

        # 範例一：每 1 分鐘執行一次
        schedules=[
            Cron("*/1 * * * *", timezone="Asia/Taipei"),  # 強制指定台灣時區
        ],

        # 範例二：每天早上 8:00 執行
        # schedules=[Cron("0 8 * * *", timezone="Asia/Taipei")],

        # 範例三：每週一到週五早上 9:30 執行
        # schedules=[Cron("30 9 * * 1-5", timezone="Asia/Taipei")],

    )