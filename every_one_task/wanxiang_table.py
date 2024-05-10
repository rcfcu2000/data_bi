from .base_action import base_action

class wanxiang_table:
    
    def __init__(self) -> None:
        self.base = base_action()
        pass
    
    
    def run(self):
        
        table_name = ['wanxiang_product', 'wanxiang_keywords', 'wanxiang_audience']
        
        for item in table_name:
            
            self.base.get_configs(item)
        
            self.base.wanxiang_table(item)
        
        print('万相台报表数据写入执行完成！')
        return True
    
    
if __name__ == '__main__':
    wanxiang = wanxiang_table()
    wanxiang.run()