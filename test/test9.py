from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# 原始 URL
url = "https://sycm.taobao.com/cc/excel.do?_path_=item/sale/sku/excel&dateType=day&dateRange=2024-04-09|2024-04-09&device=0&itemId=782198073937"

def new_url(dict_: dict, oldurl):
    
    obj = {}
    
    try:
        # 解析 URL
        parsed_url = urlparse(oldurl)
        query_params = parse_qs(parsed_url.query)
        
        for key, value in dict_.items():
            # print(key, value)
            query_params[key] = [value]
        
        # 将查询参数转换回查询字符串
        new_query = urlencode(query_params, doseq=True)
        
        # 重建 URL
        new_url = urlunparse((
            parsed_url.scheme,
            parsed_url.netloc,
            parsed_url.path,
            parsed_url.params,
            new_query,
            parsed_url.fragment
        ))
        
        obj['mark'] = True
        obj['url'] = new_url
        
    except Exception as e:
        
        obj['mark'] = False
        obj['url'] = str(e)
    
    return obj


url_ = new_url(dict_={'itemId':'8888888888', 'dateRange':'2024-04-11|2024-04-11'}, oldurl=url)

print(url_)