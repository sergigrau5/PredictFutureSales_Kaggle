import pandas as pd
from tqdm import tqdm
import numpy
import json

def getShopName(df, shop_id, shops_dict):
    if shop_id in shops_dict:
        return shops_dict, shops_dict[shop_id]
    else:
        name = df[df.shop_id == shop_id]['shop_name'].values[0]
        shops_dict.update({shop_id: name})
        return shops_dict, name

def getItemNameAndCategory(df_item, df_categories, item_id, items_dict):
    if item_id in items_dict:
        return items_dict, items_dict[item_id][0], items_dict[item_id][1]
    else:
        name = df_item[df_item.item_id == item_id]['item_name'].values[0]
        item_category_id = df_item[df_item.item_id == item_id]['item_category_id'].values[0]
        category_name = df_categories[df_categories.item_category_id == item_category_id]['item_category_name'].values[0]
        items_dict.update({item_id: [name, category_name]})
        return items_dict, name, category_name

def getMeanPriceProduct(item, shop, df, dict):

    if(not df.empty):
        item_mean = round(df['item_price'].mean(), 2)
        dict.update({
            '{};{}'.format(item, shop): item_mean
        })
    return dict

def populateWithEmptyLines(raw_data, item_price_dict, shop_id, items_dict, shops_dict, months_dict, items_list):
    with tqdm(total=len(items_list)) as pbar:
        for item_id in items_list:
            try:
                for month in months_dict['{};{}'.format(item_id, shop_id)]:
                    # df = df.append({'Shop_id': shop_id, 'Date_Block_Num': month, 'Item_id': item_id, 'Item_Name': items_dict[item_id][0],
                    #                 'Item_Category_Name': items_dict[item_id][1], 'Shop_Name': shops_dict[shop_id],
                    #                 'Item_Price': item_price_dict['{};{}'.format(item_id, shop_id)],
                    #                 'Item_Cnt_Day': 0}, ignore_index=True)
                    raw_data.append([shop_id, month, item_id, items_dict[item_id][0], items_dict[item_id][1], shops_dict[shop_id],
                                    item_price_dict['{};{}'.format(item_id, shop_id)], 0])
            except:
                continue
            pbar.update(1)
    return raw_data

def find_missing(lst):
    return [x for x in range(1, 34) if x not in lst]

def getMissingMonthsForProductAndShop(item, shop, df, dict):
    if(not df.empty):
        months = df.date_block_num.unique()
        months = find_missing(months)
        dict.update({
            '{};{}'.format(item, shop): months
        })
    return dict
