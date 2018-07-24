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
concord_table=dd.read_csv('C:\\Users\Kanishka.Narayan\Desktop\FAOData\Aggregation for crop type.csv')


data= dd.merge(data, concord_table, how="left", left_on="Item Code", right_on='Code no')

data['Series_Name']=data[str('Code Name')] +data[str('Element')]
series_concord_table=dd.read_csv('C:\\Users\Kanishka.Narayan\Desktop\FAOData\SeriesConcordance.csv')


data.columns=list(data.columns)
data= data.drop(['Area Code','Item Code','Flag','Unit','Year Code','Element','Element Code','Code','Code Name','Item','Code no'],axis=1)
print(data.head())
data=data.dropna(how='any')
print(data.head())

data.reset_index()

datapanda= data.compute()
#data=pd.DataFrame(data)
#p= datapanda.pivot_table(index=["Area",'Year'],values=['Value'],
               #columns=["Series Name in Ifs"],aggfunc=[np.sum])

p=pd.pivot_table(datapanda,index=["Area",'Year'],values=['Value'],columns=["Series_Name"],aggfunc=[np.sum])

writer= pd.ExcelWriter('FAOAggregated.xlsx',engine='xlsxwriter')
p.to_excel(writer,sheet_name='Deaths')



writer.save()
#writer= pd.csv_writer('FAOFBSAggregated.csv',engine='xlsxwriter')
#data.to_csv('C:\\Users\Kanishka.Narayan\Desktop\FAOData\Book3-*.csv')



