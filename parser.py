import pandas as pd
from tqdm import tqdm

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

print('### ...READING FILES... ### \n')

#Date, Date_Block_Num, Shop_id, Item_id, Item_price, Item_cnt_day
df_sales_train = pd.read_csv('sales_train.csv', sep=',')

#Item_Name, Item_Id, Item_Category_id
df_items = pd.read_csv('items.csv', sep=',')

#Item_category_name, Item_Category_id
df_categories = pd.read_csv('item_categories.csv', sep=',')

#Shop_Name, Shop_id
df_shops = pd.read_csv('shops.csv', sep=',')

print('### ...GROUPING REGISTERS... ###')

df_sales_train['date'] = pd.to_datetime(df_sales_train.date, dayfirst=True)
df_sales_train['date'] = df_sales_train['date'].dt.strftime('%B-%Y')

df_sales_train = df_sales_train.groupby(['shop_id', 'date', 'date_block_num', 'item_id', 'item_price'])['item_cnt_day'].sum().reset_index()

shops = {}
items = {}

cols = ['Shop_id', 'Date', 'Date_Block_Num', 'Item_id', 'Item_Name', 'Item_Category_Name', 'Shop_Name', 'Item_Price', 'Item_Cnt_Day']

raw_data = []

print('### ...MERGING INFORMATION INTO A FINAL FILE... ###\n')

with tqdm(total=len(df_sales_train.values)) as pbar:
    for row in df_sales_train.itertuples(index=False):
        shops, shopName = getShopName(df_shops, row.shop_id, shops)
        items, itemName, itemCategoryName = getItemNameAndCategory(df_items, df_categories, row.item_id, items)
        raw_data.append(list(row)[:len(row)-2] + [itemName] + [itemCategoryName] + [shopName] + [row[len(row)-2]] + [row[len(row)-1]])
        pbar.update(1)

print('### ...CREATING THE FINAL FILE... ###\n')

df = pd.DataFrame(raw_data, columns=cols)
df.to_csv('train.csv', sep=';', header=True, encoding='utf-8', index=False)

print('### ...THE PROGRAM HAS BEEN EXECUTED PROPERLY... ###\n')
