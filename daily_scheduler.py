import time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.events import EVENT_JOB_ERROR

from fund_db.fund_db_main import main as fund_db_main
from fund_db.fund_performance import main as fund_performance_main
from portfolio.portfolio_extension import main as portfolio_extension_main
from portfolio.portfolio_performance import main as portfolio_performance_main
from rpa.rpa_daily_performance import main as rpa_daily_performance_main
from rpa.rpa_portfolio_contribution import main as rpa_portfolio_contribution_main
from rpa.rpa_product_box import main as rpa_product_box_main
from monitor.fund_majoir_event import main as monitor_main
from monitor.fund_style_monitor import main as fund_style_monitor_main
from monitor.weekly_monitor import main as weekly_monitor_main

from quant_utils.logger import Logger


logger = Logger()
# 配置任务存储
jobstores = {
    "default": SQLAlchemyJobStore(url="sqlite:///jobs.sqlite")  # 使用 SQLite 数据库
}

# 初始化调度器
scheduler = BackgroundScheduler(jobstores=jobstores)


def run_func(func_list: list, *args, **kwargs):
    for func in func_list:
        logger.info(f"开始执行{func.__name__}")
        try:
            func(*args, **kwargs)
            print_str = f"{func.__name__}执行成功"
            logger.info(print_str)
        except Exception as e:
            print_str = f"{func.__name__}执行失败，错误信息为{e}"
            logger.error(print_str)
            continue
        finally:
            time.sleep(20)


def monitor():
    monitor_main()
    weekly_monitor_main()


def db_update():
    func_list = [
        fund_db_main,
        portfolio_extension_main,
        fund_performance_main,
        portfolio_performance_main,
        monitor,
    ]
    run_func(func_list)


def daily_rpa(if_send: bool = True):
    func_list = [
        rpa_daily_performance_main,
        rpa_product_box_main,
        rpa_portfolio_contribution_main,
    ]

    run_func(func_list, if_send=if_send)
    run_func([fund_style_monitor_main])


def morning_schedule(if_send: bool = True):
    db_update()
    daily_rpa(if_send)


def print_scheduled_jobs():
    """打印当前调度的任务列表"""
    if jobs := scheduler.get_jobs():
        logger.info("当前调度的任务列表:")
        for job in jobs:
            logger.info(f"任务: {job.name}, 下次运行时间: {job.next_run_time}")
    else:
        logger.info("当前没有调度任务。")


def my_listener(event):
    logger.info(f"监听到事件: {event}")
    if event.code == EVENT_JOB_ERROR:
        logger.error(f"任务 {event.job_id} 执行失败，错误信息: {event.exception}")


def schedule_main():
    # 添加任务到调度器
    scheduler.add_job(
        morning_schedule,
        "cron",
        hour=5,
        minute=0,
        id="morning_schedule_task",
        name="早间数据库更新及RPA",
        replace_existing=True,
    )
    scheduler.add_job(
        db_update,
        "cron",
        hour=20,
        minute=0,
        id="db_update_task",
        name="晚间数据库更新",
        replace_existing=True,
    )
    scheduler.add_listener(my_listener)  # 添加监听
    # 启动调度器
    scheduler.start()

    try:
        # 保持主线程运行
        while True:
            # logger.info("python_daily_scheduler is running, dont stop me")
            time.sleep(1)
            # # 打印当前调度任务
            # print_scheduled_jobs()
    except (KeyboardInterrupt, SystemExit):
        logger.error("程序被手动终止")
        # 关闭调度器
        scheduler.shutdown()


if __name__ == "__main__":
    schedule_main()
