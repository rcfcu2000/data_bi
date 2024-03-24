# -*- coding: utf-8 -*-
import re

url = "https://sycm.taobao.com/flow/excel.do?_path_=v6/excel/item/crowdtype/source/v3&belong=all&dateType=day&dateRange=2024-03-05|2024-03-05&crowdType=all&device=2&itemId=761041987303&itemId=761041987303&device=2&order=desc&orderBy=uv"

# 使用正则表达式提取日期范围
match = re.search(r'dateRange=(\d{4}-\d{2}-\d{2}\|\d{4}-\d{2}-\d{2})', url)

if match:
    original_date_range = match.group(1)
    print("原始日期范围:", original_date_range)

    # 替换日期范围为你想要的时间，比如2024-03-01到2024-03-10
    new_date_range = "2024-03-01|2024-03-10"
    modified_url = re.sub(r'dateRange=\d{4}-\d{2}-\d{2}\|\d{4}-\d{2}-\d{2}', f'dateRange={new_date_range}', url)

    print("修改后的链接:", modified_url)
    
else:
    
    print("未找到日期范围")
    

