from lib.utils import *
import pandas as pd
from tqdm import tqdm
import numpy
import json


if __name__== "__main__":
    print('### ...READING FILES... ###')

    #Date, Date_Block_Num, Shop_id, Item_id, Item_price, Item_cnt_day
    df_sales_train = pd.read_csv('data/sales_train.csv', sep=',')

    #Item_Name, Item_Id, Item_Category_id
    df_items = pd.read_csv('data/items.csv', sep=',')

    #Item_category_name, Item_Category_id
    df_categories = pd.read_csv('data/item_categories.csv', sep=',')

    #Shop_Name, Shop_id
    df_shops = pd.read_csv('data/shops.csv', sep=',')

    item_price_dict = {}
    unique_months_dict = {}

    items_li = df_items.item_id.unique()
    shops_li = df_shops.shop_id.unique()

    '''
    print('### ...CALCULATING MEAN OF ITEM PRICES && EMPTY MONTHS FOR EACH PRODUCT IN EACH SHOP... ###')

    with tqdm(total=len(items_li) * len(shops_li)) as pbar:
        for shop in shops_li:
            for item in items_li:
                df = df_sales_train[(df_sales_train.item_id == item) & (df_sales_train.shop_id == shop)]
                item_price_dict = getMeanPriceProduct(item, shop, df, item_price_dict)
                unique_months_dict = getMissingMonthsForProductAndShop(item, shop, df, unique_months_dict)
                pbar.update(1)

    print('### ...DUMPING MEAN PRICES INTO A JSON FILE... ###')

    with open('item_prices.json', 'w') as fp_items:
        json.dump(item_price_dict, fp_items)

    print('### ...DUMPING MISSING MONTHS INTO A JSON FILE... ###')

    with open('unique_months.json', 'w') as fp_months:
        json.dump(unique_months_dict, fp_months)
    '''

    print('### ...GROUPING REGISTERS... ###')

    df_sales_train['date'] = pd.to_datetime(df_sales_train.date, dayfirst=True)
    df_sales_train['date'] = df_sales_train['date'].dt.strftime('%B-%Y')

    df_sales_train = df_sales_train.groupby(['shop_id', 'date', 'date_block_num', 'item_id', 'item_price'])['item_cnt_day'].sum().reset_index()

    print('### ...MERGING INFORMATION INTO A FINAL FILE... ###\n')

    shops_info_dict = {}
    items_info_dict = {}

    cols = ['Shop_id', 'Date', 'Date_Block_Num', 'Item_id', 'Item_Name', 'Item_Category_Name', 'Shop_Name', 'Item_Price', 'Item_Cnt_Day']

    raw_data = []

    with tqdm(total=len(df_sales_train.values)) as pbar:
        for row in df_sales_train.itertuples(index=False):
            shops_info_dict, shopName = getShopName(df_shops, row.shop_id, shops_info_dict)
            items_info_dict, itemName, itemCategoryName = getItemNameAndCategory(df_items, df_categories, row.item_id, items_info_dict)
            raw_data.append(list(row)[:len(row)-2] + [itemName] + [itemCategoryName] + [shopName] + [row[len(row)-2]] + [row[len(row)-1]])
            pbar.update(1)

    df = pd.DataFrame(raw_data, columns=cols)

    print('### ...LOADING JSON FILES... ###')

    with open('data/item_prices.json') as json_file:
        item_price_dict = json.load(json_file)

    with open('data/unique_months.json') as json_file:
        unique_months_dict = json.load(json_file)

    print('### ...ADDING EMPTY LINES... ###')

    raw_data = []

    cols = ['Shop_id', 'Date_Block_Num', 'Item_id', 'Item_Name', 'Item_Category_Name', 'Shop_Name', 'Item_Price', 'Item_Cnt_Day']

    with tqdm(total=len(shops_li)) as pbar:
        for shop in shops_li:
            raw_data = populateWithEmptyLines(raw_data, item_price_dict, shop, items_info_dict,
                                              shops_info_dict, unique_months_dict, items_li)
            pbar.update(1)

    print('### ...CREATING THE FINAL FILE... ###\n')

    df_2 = pd.DataFrame(raw_data, columns=cols)

    df_final = pd.concat([df, df_2])

    df_final.to_csv('train_final.csv', sep=';', header=True, encoding='utf-8', index=False)

    print('### ...THE PROGRAM HAS BEEN EXECUTED PROPERLY... ###\n')
