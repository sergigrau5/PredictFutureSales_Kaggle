import pandas as pd
import matplotlib as plt

def getGraphByProductAndShop(df, item):
    df_item = df[df.Item_id == item]
    df_item.plot(kind='line', x='Date_Block_Num', y='Item_Cnt_Day', color='red')
    pl.show()
    exit()

df = pd.read_csv('train_5_empty.csv', sep=';')
getGraphByProductAndShop(df, 77)
