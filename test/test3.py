import re

url = "https://sycm.taobao.com/flow/excel.do?_path_=v6/excel/item/crowdtype/source/v3&belong=all&dateType=day&dateRange=2024-03-05|2024-03-05&crowdType=all&device=2&itemId=761041987303&itemId=761041987303&device=2&order=desc&orderBy=uv"

# 使用正则表达式提取itemid
item_ids = re.findall(r'itemId=(\d+)', url)

if item_ids:
    original_item_ids = item_ids
    print("原始itemIds:", original_item_ids)

    # 替换itemIds为你想要的ID
    new_item_ids = ["your_desired_id1", "your_desired_id2"]
    modified_url = re.sub(r'itemId=\d+', lambda x: f'itemId={new_item_ids.pop(0)}', url)

    print("修改后的链接:", modified_url)
else:
    print("未找到itemIds")
