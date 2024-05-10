def download_items_extra_rank(href, dstring):
        d_name = '【生意参谋平台】商品_全部_' + end_date + '_' + end_date + '.xls'
        filename = default_file_path + d_name
        href = "https://sycm.taobao.com/domain/oneQuery.json?extMap=%7B%22itemIdStr%22%3A%22"
        

        excel_data_df = pd.read_excel(filename, usecols="B,E", header=4)
        
        excel_data_df = excel_data_df.sort_values(by=['商品ID'], ascending=False)
        
        good_lists = []
        counter = 0
        itemidstr = ''
        total_counter = 0
        print("getting extra_rank for " + str(len(excel_data_df)) + " goods......")
        for i in range(0, len(excel_data_df)):
            good_id = str(excel_data_df.iloc[i].values[0])
            good_status = excel_data_df.iloc[i].values[1]
            total_counter += 1
            if good_id in good_lists or good_status == '已下架':
                    print("skip getting extra_rank for ", good_id, str(total_counter))
                    continue
            print("getting extra_rank for ", good_id, str(total_counter))
            counter += 1
            if counter >= 10:
                href += itemidstr + "%22%7D&domainCode=tao.shop.shop.item&dateType=day&dateRange=" + dstring + '%7C' + dstring + "&bizCode=sycm_pc&showType=list&device=0&indexCodes=starLevel001%2CitemUnitPrice1%2CitmDstPrice%2Cpv1dCtr%2CitemId"
                print(href)
                driver.get(href)
                sleep(3 + random.randint(1, 10))
                with open(default_file_path + dstring + '_' + str(total_counter) + '_items_extra_rank.json', 'w') as f:
                   f.write(driver.find_element(By.TAG_NAME,'body').text)
                counter = 0
                itemidstr = ''
                href = "https://sycm.taobao.com/domain/oneQuery.json?extMap=%7B%22itemIdStr%22%3A%22"
            else:
                  if counter > 1:
                    itemidstr += '%2C' + good_id 
                  else:
                    itemidstr += good_id

        return True
