import pandas as pd
from tqdm import tqdm

def populateWithEmptyLines(df, item_price_dict, shop, items_dict, shops_dict):
    items = df.Item_id.unique()
    with tqdm(total=len(items)) as pbar:
        for item in items:
            months = df[df.Item_id == item].Date_Block_Num.unique()
            months = find_missing(months)
            for month in months:
                df = df.append({'Shop_id': 5, 'Date_Block_Num': month, 'Item_id': item, 'Item_Name': items_dict[item][0],
                                'Item_Category_Name': items_dict[item][1], 'Shop_Name': shops_dict[shop],
                                'Item_Price': item_price_dict['{};5'.format(item)],
                                'Item_Cnt_Day': 0}, ignore_index=True)
            pbar.update(1)

    return df

def find_missing(lst):
    return [x for x in range(1, 33) if x not in lst]

def getMeanPriceProduct(item, shop, df, dict):
    df = df[(df.Item_id == item) & (df.Shop_id == shop)]
    item_mean = round(df['Item_Price'].mean(), 2)
    dict.update({
        '{};{}'.format(item, shop): item_mean
    })
    return dict

print('### ...READING FILES... ###')

df = pd.read_csv('trainShop5.csv', sep=';')

#Item_Name, Item_Id, Item_Category_id
df_items = pd.read_csv('items.csv', sep=',')

#Item_category_name, Item_Category_id
df_categories = pd.read_csv('item_categories.csv', sep=',')

#Shop_Name, Shop_id
df_shops = pd.read_csv('shops.csv', sep=',')

print('### ...OBTAINING ATTRIBUTES... ###')

items = {}
shops = {}

shops_list = df['Shop_id'].unique()
items_list = df['Item_id'].unique()

with tqdm(total=len(items_list)) as pbar:
    for item in items_list:
        tmp = df_items[df_items.item_id == item]
        name = tmp['item_name'].values[0]
        category_name = df_categories[df_categories.item_category_id == tmp['item_category_id'].values[0]]['item_category_name'].values[0]
        items.update({
            item: [name, category_name]
        })
        pbar.update(1)

with tqdm(total=len(shops_list)) as pbar:
    for shop in shops_list:
        shops.update({
            shop: df_shops[df_shops.shop_id == shop]['shop_name'].values[0]
        })
        pbar.update(1)

print('### ...CALCULATING MEAN OF ITEM PRICES... ###')

item_price_dict = {}
items_li = df.Item_id.unique()

with tqdm(total=len(items_li)) as pbar:
    for item in items_li:
        item_price_dict = getMeanPriceProduct(item, 5, df, item_price_dict)
        pbar.update(1)

print('### ...ADDING EMPTY LINES... ###')

#shops = df.Shop_id.unique()

#for shop in shops:
df = populateWithEmptyLines(df, item_price_dict, 5, items, shops)
#pbar.update(1)

print('### ...EXPORTING THE FILE... ###')

df.to_csv('train_5_empty.csv', sep=';', header=True, encoding='utf-8', index=False)

print('### ...THE PROGRAM HAS BEEN EXECUTED PROPERLY... ###\n')
