import requests
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import csv
import xlrd
import matplotlib.lines as mlines
import matplotlib.transforms as mtransforms
import xlsxwriter
import dask.dataframe as dd
import statsmodels.api as sm



data=dd.read_csv('C:\\Users\Kanishka.Narayan\Desktop\FAOData\FoodBalanceSheets_E_All_Data_(Normalized).csv',encoding="ISO-8859-1")
#data=pd.concat(tp,ignore_index=True)


data['Code'] = data[str('Element Code')] + data[str('Item Code')]
concord_table=dd.read_csv('C:\\Users\Kanishka.Narayan\Desktop\FAOData\AggregationforFish.csv')

data= dd.merge(data, concord_table, how="left", left_on="Code", right_on='Code in Source')


data= data.drop(['Area Code','Item Code','Flag','Unit','Year Code','Element','Element Code','Code','Item'],axis=1)

data=data.dropna(how='any')
data.reset_index()

datapanda= data.compute()
print(datapanda.head())
p=pd.pivot_table(datapanda,index=["Area",'Year'],values=['Value'],columns=["Variable"],aggfunc=[np.sum])

writer= pd.ExcelWriter('FAOFBSFish.xlsx',engine='xlsxwriter')
p.to_excel(writer,sheet_name='Fish',merge_cells=False)

writer.save()
