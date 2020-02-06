import pandas as pd
import category_encoders as ce
from pandas import concat
from tqdm import tqdm
from sklearn.preprocessing import MinMaxScaler


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

def getTimeSeriesDataSet(df, shop, item, timeseries, df_final):
    sorted_df = df[(df.Shop_id == shop) & (df.Item_id == item)].sort_values('Date_Block_Num')

    del sorted_df['Date_Block_Num']
    del sorted_df['Shop_Name']
    del sorted_df['Item_Name']
    del sorted_df['Shop_id']
    del sorted_df['Item_id']

    df_final = df_final.append(series_to_supervised(sorted_df, timeseries))

    return df_final


print('# ...READING FILE... #')

df = pd.read_csv('train_5_empty.csv', sep=';')

'''df['Item_Name'] = df.Item_Name.astype("category").cat.codes
df['Shop_Name'] = df.Shop_Name.astype("category").cat.codes
df['Item_Category_Name'] = df.Item_Category_Name.astype("category").cat.codes
print(df.corr())'''

print('# ...DOING NORMALIZATION OF CATEGORICAL FEATURES... #')

binary_encoder = ce.BinaryEncoder(cols = ['Item_Category_Name'])
df = binary_encoder.fit_transform(df)

print('# ...DOING NORMALIZATION OF NUMERICAL FEATURES... #')

max = df['Item_Price'].max()
min = df['Item_Price'].min()
df['Item_Price'] = round((df['Item_Price'] - min) / (max - min), 3)
print('Max ' + 'Item_Price' + ': ' + str(max) + ' and Min : '+ str(min))

print('# ...GENERATING TIMESERIES ON THE DATASET... #')

shops = df.Shop_id.unique()
items = df.Item_id.unique()

#cols = ['Item_Category_Name', 'Item_Price', 'Item_Cnt_Day']

with tqdm(total=len(items)*12) as pbar:
    for element in range(1,13):
        df_final = pd.DataFrame([])
        for shop in shops:
            for item in items:
                df_final = getTimeSeriesDataSet(df, shop, item, element, df_final)
                pbar.update(1)
        df_final.to_csv('train_final_{}_series.csv'.format(element), sep=';')
