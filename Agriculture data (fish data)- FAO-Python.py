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


#Step 1: Read in master file with all data
# Please download the master csv file from the FAO website. The file name is "FoodBalanceSheets_E_All_Data_(Normalized).csv"
#Replace the path below with a path to the file in your local machine
data=dd.read_csv('C:\\Users\Kanishka.Narayan\Desktop\FAOData\FoodBalanceSheets_E_All_Data_(Normalized).csv',encoding="ISO-8859-1")

#Step 2: Create a code column 
data['Code'] = data[str('Element Code')] + data[str('Item Code')]

#Step 3: Read in concordance table  for aggregation
#This concordance table can be found in the Data folder
concord_table=dd.read_csv('C:\\Users\Kanishka.Narayan\Desktop\FAOData\AggregationforFish.csv')

#Step 4: Merge with concordance table
data= dd.merge(data, concord_table, how="left", left_on="Code", right_on='Code in Source')

#Step 5: Drop all irrelevant Columns
data= data.drop(['Area Code','Item Code','Flag','Unit','Year Code','Element','Element Code','Code','Item'],axis=1)

#Step 6: Drop all null values and reset index
data=data.dropna(how='any')
data.reset_index()

# Step 7: Convert dask data frame to regular data frame
datapanda= data.compute()
print(datapanda.head())

# Step 8: Create pivot table with relevant values
p=pd.pivot_table(datapanda,index=["Area",'Year'],values=['Value'],columns=["Variable"],aggfunc=[np.sum])

# Step 9: Write to excel 
writer= pd.ExcelWriter('FAOFBSFish.xlsx',engine='xlsxwriter')
p.to_excel(writer,sheet_name='Fish',merge_cells=False)

writer.save()
