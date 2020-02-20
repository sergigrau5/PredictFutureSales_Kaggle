import pandas as pd
import category_encoders as ce
from pandas import concat
from tqdm import tqdm, trange
import json
from sklearn.preprocessing import MinMaxScaler
from keras.preprocessing.sequence import TimeseriesGenerator
import numpy as np


def series_to_supervised(data, n_in=1, n_out=1, dropnan=True):
    """

    Arguments:
      data: Sequence of observations as a list or NumPy array.
      n_in: Number of lag observations as input (X).
      n_out: Number of observations as output (y).
      dropnan: Boolean whether or not to drop rows with NaN values.
    Returns:
      Pandas DataFrame of series framed for supervised learning.

    """
    n_vars = 1 if type(data) is list else data.shape[1]

    df = data
    cols, names = list(), list()

    # input sequence (t-n, ... t-1)
    for i in range(n_in, 0, -1):
      cols.append(df.shift(i))
      names += [('var%d(t-%d)' % (j+1, i)) for j in range(n_vars)]

    # forecast sequence (t, t+1, ... t+n)
    for i in range(0, n_out):
      cols.append(df.shift(-i))
      if i == 0:
        names += [('var%d(t)' % (j+1)) for j in range(n_vars)]
      else:
        names += [('var%d(t+%d)' % (j+1, i)) for j in range(n_vars)]

    # put it all together
    agg = concat(cols, axis=1)
    agg.columns = names

    # drop rows with NaN values
    if dropnan:
      agg.dropna(inplace=True)
    return agg

def getTimeSeriesDataSet(df, shop, item, timesteps, raw_list):
    item_df = df[(df.Shop_id == shop) & (df.Item_id == item)]

    del item_df['Date_Block_Num']
    del item_df['Shop_Name']
    del item_df['Item_Name']
    del item_df['Shop_id']
    del item_df['Item_id']

    if(len(raw_list) == 0):
        raw_list = series_to_supervised(item_df, timesteps).values
    else:
        raw_list = np.concatenate([raw_list, series_to_supervised(item_df, timesteps).values])

    return raw_list

print('# ...READING FILE... #')

df = pd.read_csv('data/train_final.csv', sep=';').sort_values(by=['Shop_id', 'Item_id', 'Date_Block_Num'])

del df['Date']

print('# ...DOING NORMALIZATION OF CATEGORICAL FEATURES... #')

binary_encoder = ce.BinaryEncoder(cols = ['Item_Category_Name'])
df = binary_encoder.fit_transform(df)

print('# ... GENERATING MAX AND MIN ITEM PRICE... #')
max = df['Item_Price'].max()
min = df['Item_Price'].min()

print('# ...DOING NORMALIZATION OF NUMERICAL FEATURES... #')
df['Item_Price'] = round((df['Item_Price'] - min) / (max - min), 3)
print('Max ' + 'Item_Price' + ': ' + str(max) + ' and Min : '+ str(min))

print('# ...GENERATING TIMESERIES ON THE DATASET... #')

shops_li = df.Shop_id.unique()
items_li = df.Item_id.unique()

#cols = ['Item_Category_Name', 'Item_Price', 'Item_Cnt_Day']

with open('data/unique_months.json') as json_file:
    unique_months_dict = json.load(json_file)

raw_data_final = []
with tqdm(total=12) as pbar_files:
    for element in range(1, 13):
        train_final = []
        with tqdm(total=len(shops_li)) as pbar_shops:
            for shop in shops_li:
                with tqdm(total=len(items_li)) as pbar_items:
                    for item in items_li:
                        key = '{};{}'.format(item,shop)
                        if key in unique_months_dict:
                            train_final = getTimeSeriesDataSet(df, shop, item, element, train_final)
                        pbar_items.update(1)
                pbar_shops.update(1)
        pbar_files.update(1)
        print('# ...WRITTING FILE OF SERIE {}... # '.format(element))
        df_final = pd.DateFrame(train_final)
        df_final.to_csv('train_final_{}_series.csv'.format(element), sep=';', index=False)
