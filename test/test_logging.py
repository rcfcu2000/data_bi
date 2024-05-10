import logging

# 配置日志输出的格式
logging.basicConfig(
    filename="app.log",
    format='%(asctime)s - %(levelname)s - %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO  # 设置日志级别为 INFO
)

# 记录日志信息
logging.debug('这是调试信息')
logging.info('这是一条普通信息')
logging.warning('这是一个警告')
logging.error('这是一个错误')
logging.critical('这是一个严重错误')
