import time
import timer as timer
import concurrent.futures
import every_one_task.base_action as ba
import every_one_task.commodity_everyday_data as ced
import every_one_task.crowd_top10 as ctop10
import every_one_task.crowd_top20 as ctop20
import every_one_task.commodity_traffic as ct
import every_one_task.crowd as crowd
import every_one_task.search_ranking as sr
import every_one_task.shop_key_words_hand_search as skw
import every_one_task.shop_key_words_through_train as skt
import every_one_task.shop_traffic as st
import every_one_task.wanxiang_table as wt
import every_one_task.wanxiangtable_keywords_everyday as wte
import every_one_task.wanxiangtable_audience_everyday as wae
import every_one_task.wanxiangtable_product_everyday as wpe
import every_one_task.commodity_sku as sku
import every_one_task.damopan_huopindongcha as dmp_hpdc
import every_one_task.price_force as pf
import every_one_task.commodity_keywords_hand_search as ckhs
import every_one_task.commodity_keywords_train as ckt
import every_one_task.biz_shop_experience_score as bses
import every_one_task.biz_product_dayinfo as bpd
import every_one_task.biz_shop_content as bsc

# 程序是立即执行还是定时执行 immediate_execution代表立即执行, timed_execution 代表定时执行
pattern = 'immediate_execution'

# 月度数据执行开关
month_execution_switch = False

bact = ba.base_action()
        
config_names = bact.get_config_name()

def run_task(item):
    
    # 商品每日数据
    ced.commodity_everyday_data(config=item).test()
    
    # # 店铺流量数据
    # st.shop_traffic(config=item).run()

    # # 商品流量数据
    # ct.commodity_traffic(config=item).run()
    
    # # 店铺关键词[手淘搜索]
    # skw.shop_key_words_hand_search(config=item).run()
    
    # # 店铺关键词[直通车]
    # skt.shop_key_words_through_train(config=item).run()
    
    # # 搜索排行 [行业数据]
    # sr.search_ranking(config=item).run()
    
    # # 万相台[ 每日 ]
    # wte.wanxiangtable_keywords_everyday(config=item).run()
    
    # # 万相台 [人群]
    # wae.wanxiangtable_audience_everyday(config=item).run()
            
    # 万相台 [宝贝主体]
    # wpe.wanxiangtable_product_everyday(config=item).run()
            
    # sku 销售详情
    # sku.commodity_sku(config=item).run()
            
    # 达摩盘 货品洞察
    # dmp_hpdc.damopan_huopindongcha(config=item).run()
    
    # 价格力
    # pf.price_force(config=item).run()
    
    # 单品 [手淘搜索]
    # ckhs.commodity_keywords_hand_search(config=item).run()
    
    # 单品 [直通车]
    # ckt.commodity_keywords_train(config=item).run()
    
    # 店铺等级与排名
    bpd.biz_product_dayinfo(config=item).run()
    
    # 内容渠道效果
    bsc.biz_shop_content(config=item).run()
    
    # 商品体验分
    # bses.biz_shop_experience_score(config=item).run()
            
    # 月度数据
    if month_execution_switch:
                
        # 人群  top10
        ctop10.crowd_top10(config=item).run()
                
        # 人群20
        ctop20.crowd_top20(config=item).run()
                
        # 人群
        crowd.crowd(config=item).run()
    
    pass

def running(mode='sequence'):
    # 顺序执行模式
    if mode == 'sequence':
        
        for item in config_names:
            
            run_task(item)
                
    elif mode == 'timed_execution':
        # 定时执行模式
        
        t = timer.Timer()
        commodity_everyday_data_ = commodity_everyday_data.commodity_everyday_data()
        crowd_top10_ = crowd_top10.crowd_top10()
        task = [ctop10.run, ced.run]
        type_ = ['cron', 'cron']
        hour = [17, 17]
        minute = [52, 53]
        shed = t.everyday_time_run(task, type_, hour=hour, minute=minute)
        try:
            print('测试')
            # 在这里做其他事情
            while True:
                time.sleep(2)  # 主线程等待，确保调度器有足够的时间来执行任务
        except (KeyboardInterrupt, SystemExit):
            shed.shutdown()
    
    else:
        # 多线程执行模式
        # 创建线程池，指定最大线程数
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            
            # 提交多个任务到线程池
            futures = [executor.submit(run_task, config_names[i]) for i in range(len(config_names))]
        
            # 等待所有任务完成
            concurrent.futures.wait(futures)
         

if __name__ == '__main__':
    
    running('sequence')