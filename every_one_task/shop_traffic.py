"""
    店铺流量来源
"""
import random
import json
import re
import pandas as pd
import calendar
from .base_action import base_action
from datetime import datetime, timedelta

class shop_traffic:
    
    def __init__(self, config) -> None:
        self.base = base_action()
        self.config = config
        pass
    
    # 由于最早在 base_action 里面已经写好了该类的方法， 故直接调用base_action里面的方法， 不再另做修改, 只做提任务类提取处理.
    
    def run(self):
        
        self.base.sycm_shop_flow_source(self.config)
        
        print(f'{self.base.config_obj["shop_name"]}: <info> 执行完毕 [店铺流量来源]!')
    
if __name__ == "__main__":
    test = shop_traffic()
    test.run()
        