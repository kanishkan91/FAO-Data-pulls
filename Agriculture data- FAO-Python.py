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

#Step 1: Read in master data file
# Download the latest version of this file from the FAO website. The file name is "FoodBalanceSheets_E_All_Data_(Normalized).csv"
#Replace path below with path on your local machine
data=dd.read_csv('C:\\Users\Kanishka.Narayan\Desktop\FAOData\FoodBalanceSheets_E_All_Data_(Normalized).csv',encoding="ISO-8859-1")
#data=pd.concat(tp,ignore_index=True)

#Step 2: Create a code column
data['Code'] = data[str('Element Code')] + data[str('Item Code')]

#Step 3: Read in crop concordance table
concord_table=dd.read_csv('C:\\Users\Kanishka.Narayan\Desktop\FAOData\Aggregation for crop type.csv')

#Step 4: Merge in concordance table
data= dd.merge(data, concord_table, how="left", left_on="Item Code", right_on='Code no')

#Step 5: Create a series name column
data['Series_Name']=data[str('Code Name')] +data[str('Element')]

#Step 6: Read in series concordance table
series_concord_table=dd.read_csv('C:\\Users\Kanishka.Narayan\Desktop\FAOData\SeriesConcordance.csv')

# Step 7: Drop unwanted columns and all null values
data.columns=list(data.columns)
data= data.drop(['Area Code','Item Code','Flag','Unit','Year Code','Element','Element Code','Code','Code Name','Item','Code no'],axis=1)
print(data.head())
data=data.dropna(how='any')
print(data.head())

#Step 8: reset index and convert back to regular data frame
data.reset_index()
datapanda= data.compute()

#Step 9: Create a pivot table with relevant values
p=pd.pivot_table(datapanda,index=["Area",'Year'],values=['Value'],columns=["Series_Name"],aggfunc=[np.sum])

#Step 10: Write to excel
writer= pd.ExcelWriter('FAOAggregated.xlsx',engine='xlsxwriter')
p.to_excel(writer,sheet_name='Deaths')



writer.save()
#writer= pd.csv_writer('FAOFBSAggregated.csv',engine='xlsxwriter')
#data.to_csv('C:\\Users\Kanishka.Narayan\Desktop\FAOData\Book3-*.csv')



