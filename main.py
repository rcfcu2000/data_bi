import time

import base_action
import timer

if __name__ == '__main__':
    t = timer.Timer()
    e = labipaiRPA.labipaiRPA()
    e1 = labipaiRPA.labipaiRPA()
    task = [e.test, e1.test1]
    type_ = ['cron', 'cron']
    hour = [15, 15]
    minute = [23, 24]
    shed = t.everyday_time_run(task, type_, hour=hour, minute=minute)
    try:
        print('测试')
        # 在这里做其他事情
        while True:
            time.sleep(2)  # 主线程等待，确保调度器有足够的时间来执行任务
    except (KeyboardInterrupt, SystemExit):
        shed.shutdown()
