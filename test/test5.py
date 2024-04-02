# -*- coding: utf-8 -*-

from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

url = "https://sycm.taobao.com/flow/excel.do?_path_=v6/excel/item/crowdtype/source/v3&belong=all&dateType=day&dateRange=2024-03-05|2024-03-05&crowdType=all&device=2&itemId=761041987303&itemId=761041987303&device=2&order=desc&orderBy=uv"

# 解析URL并提取查询参数
parsed_url = urlparse(url)
query_params = parse_qs(parsed_url.query)

# 获取belong参数的值，如果存在则进行修改
belong_param = query_params.get('belong', None)
if belong_param:
    new_belong_value = "your_new_value"
    query_params['belong'] = [new_belong_value]

# 构建新的URL
new_query_string = urlencode(query_params, doseq=True)
new_url_parts = parsed_url._replace(query=new_query_string)
new_url = urlunparse(new_url_parts)

print("修改后的URL是:", new_url)
