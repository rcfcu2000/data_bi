"""
    定时器
"""
import base_action
from datetime import date, datetime
from apscheduler.schedulers.background import BackgroundScheduler


class Timer:
    def __init__(self):
        pass

# 每日定时执行
    def everyday_time_run(self, func: list, type_: list, hour: list, minute: list):

        scheduler = BackgroundScheduler()

        for i in range(0, len(func)):

            scheduler.add_job(func[i], type_[i], hour=hour[i], minute=minute[i])

        scheduler.start()

        return scheduler

        pass

