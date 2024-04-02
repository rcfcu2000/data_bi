import time
<<<<<<< HEAD
import timer.timer as timer
import every_one_task.commodity_everyday_data as commodity_everyday_data
import every_one_task.crowd_top10 as crowd_top10

def run():
    
    pass

if __name__ == '__main__':
    t = timer.Timer()
    commodity_everyday_data_ = commodity_everyday_data.commodity_everyday_data()
    crowd_top10_ = crowd_top10.crowd_top10()
    task = [crowd_top10_.run, commodity_everyday_data_.run]
    type_ = ['cron', 'cron']
    hour = [17, 17]
    minute = [52, 53]
=======

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
>>>>>>> cd5a759c91eaf1f85c123e8bf256658f36943fcb
    shed = t.everyday_time_run(task, type_, hour=hour, minute=minute)
    try:
        print('测试')
        # 在这里做其他事情
        while True:
            time.sleep(2)  # 主线程等待，确保调度器有足够的时间来执行任务
    except (KeyboardInterrupt, SystemExit):
        shed.shutdown()
