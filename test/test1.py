import concurrent.futures
import threading
import time


class MyClass:
    thread_local_data = threading.local()

    def set_attribute(self, value):
        # 设置线程本地变量
        self.thread_local_data.attribute = value

    def get_attribute(self):
        # 获取线程本地变量
        return getattr(self.thread_local_data, 'attribute', None)

    def modify_attribute(self):
        for _ in range(5):
            current_value = self.get_attribute()
            time.sleep(0.1)  # 模拟一些工作
            self.set_attribute(current_value + 1)
            print(f"Thread {threading.current_thread().name}: {self.get_attribute()}")


def main():
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        # 提交两个任务给线程池
        future1 = executor.submit(MyClass().modify_attribute)
        future2 = executor.submit(MyClass().modify_attribute)

        # 等待任务完成
        concurrent.futures.wait([future1, future2])


if __name__ == "__main__":
    main()

