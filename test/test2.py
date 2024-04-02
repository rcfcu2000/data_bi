from datetime import datetime, timedelta

# 获取当前日期
current_date = datetime.now()

# 计算前一天日期
previous_date = current_date - timedelta(days=1)

# 打印结果
print("当前日期:", current_date.strftime("%Y-%m-%d"))
print("前一天日期:", previous_date.strftime("%Y-%m-%d"))
