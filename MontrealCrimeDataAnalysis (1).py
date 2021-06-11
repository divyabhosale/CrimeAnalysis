# Databricks notebook source

#Initial Setup
import numpy as np
import pandas as pd
import datetime as dt
import calendar as cl
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# COMMAND ----------


#Creating temporary/local table
spark.read.format("csv").option("header","true")\
  .option("inferSchema", "true")\
  .option("encoding","latin1")\
  .load("/FileStore/tables/interventionscitoyendo.csv")\
  .createOrReplaceTempView("MontrealCrimeData")

# COMMAND ----------

#Converting the spark dataframe to Pandas to work on the column operations
montreal=spark.table("MontrealCrimeData")
pdMontreal=montreal.toPandas()

# COMMAND ----------

#Data Cleaning

#Renaming column
pdMontreal.rename(columns={"CATEGORIE":"CATEGORY"},inplace=True)
pdMontreal.dtypes

#Converting the CATEGORIE text to English
pdMontreal["CATEGORY"]=pdMontreal["CATEGORY"].replace("Vol de véhicule à moteur","Motor vehicle theft")\
                                                .replace("Méfait","Misdemeanor(Mischiefs)")\
                                                .replace("Vols qualifiés","Qualified flights(Robbery)")\
                                                .replace("Vol dans / sur véhicule à moteur","Flight into / on motor vehicle")\
                                                .replace("Introduction","Introduction(Breaking)")\
                                                .replace("Infractions entrainant la mort","Offenses causing death")

#Converting the QUART text to English
pdMontreal["QUART"]=pdMontreal["QUART"].replace("jour","Day")\
                                        .replace("nuit","Night")\
                                        .replace("soir","Evening")
#pdMontreal.dtypes #for getting the data types of columns

# COMMAND ----------

#Data Transformation

#Diving the dataframe to work on the year wise crime data
pdMontrealYearWise=pdMontreal[["CATEGORY","DATE"]]

#Creating a new column year
pdMontrealYearWise["YEAR"]=pdMontrealYearWise["DATE"].dt.to_period('Y').astype(str)
pdMontrealYearWise["COUNT"]=pdMontrealYearWise.groupby(['CATEGORY','YEAR']).transform(np.size)
pdMontrealYearWise.drop(['DATE'], axis=1, inplace=True)
pdMontrealYearWise=pdMontrealYearWise.drop_duplicates()
pdMontrealYearWise=pdMontrealYearWise.sort_values('YEAR')

# COMMAND ----------

#Data Visualization
pdMontrealYearWise=spark.createDataFrame(pdMontrealYearWise)
display(pdMontrealYearWise)

# COMMAND ----------

#Data Transformation

#Dividing the dataframe to work on the month wise crime data
pdMontrealMonthWise=pdMontreal[["CATEGORY","DATE"]]

#Dividing teh dataframe to work on MonthWise Season crime data
pdMontrealMonthWiseSeason = pdMontreal[["CATEGORY" ,"PDQ","DATE"]]

#Creating a new column month
pdMontrealMonthWise["MONTH"]=pdMontrealMonthWise["DATE"].dt.to_period('M').astype(str)
pdMontrealMonthWise['MONTH']  = pdMontrealMonthWise['MONTH'].str.split('-').str[1]
pdMontrealMonthWise['MONTH'] = pdMontrealMonthWise['MONTH'].apply(pd.to_numeric)
result = [] 
for value in pdMontrealMonthWise['MONTH']: 
    if value >= 4 and value <= 7: 
        result.append("Summer") 
    elif value >= 7 and value <= 10: 
        result.append("Fall") 
    else:
        result.append("Winter") 
       
pdMontrealMonthWise['RESULT'] = result
pdMontrealMonthWiseSeason['SEASON'] = result

#Data transforming for Month wise visualization
pdMontrealMonthWise["COUNT"]=pdMontrealMonthWise.groupby(['CATEGORY','MONTH','RESULT']).transform(np.size)
pdMontrealMonthWise.drop(['DATE'], axis=1, inplace=True)
pdMontrealMonthWise=pdMontrealMonthWise.drop_duplicates()
pdMontrealMonthWise=pdMontrealMonthWise.sort_values('MONTH')

# COMMAND ----------

#Data Visualization
pdMontrealMonthWise=spark.createDataFrame(pdMontrealMonthWise)
display(pdMontrealMonthWise)

# COMMAND ----------

#Data Transformation

#Dividing the dataframe to work on quart wise total crime data
pdMontrealQuartWiseTotal=pdMontreal[["CATEGORY","QUART"]]
pdMontrealQuartWiseTotal["COUNT"]=pdMontrealQuartWiseTotal.groupby(['QUART']).transform(np.size)
pdMontrealQuartWiseTotal.drop(['CATEGORY'], axis=1, inplace=True)
pdMontrealQuartWiseTotal=pdMontrealQuartWiseTotal.drop_duplicates()

# COMMAND ----------

#Data Visualization
pdMontrealQuartWiseTotal=spark.createDataFrame(pdMontrealQuartWiseTotal)
display(pdMontrealQuartWiseTotal)

# COMMAND ----------

#Data Transformation

#Dividing the dataframe to work on quart wise crime data by category of offence
pdMontrealQuartWiseCategory=pdMontreal[["CATEGORY","QUART","DATE"]]
pdMontrealQuartWiseCategory["COUNT"]=pdMontrealQuartWiseCategory.groupby(['CATEGORY','QUART']).transform(np.size)
pdMontrealQuartWiseCategory.drop(['DATE'], axis=1, inplace=True)
pdMontrealQuartWiseCategory=pdMontrealQuartWiseCategory.drop_duplicates()

# COMMAND ----------

#Data Visualization
pdMontrealQuartWiseCategory=spark.createDataFrame(pdMontrealQuartWiseCategory)
display(pdMontrealQuartWiseCategory)

# COMMAND ----------

#Data Transformation

#Extracting information from the date column to get weekdays
pdMontrealDays =pdMontreal[['QUART','CATEGORY','DATE']]
pdMontrealDays['WEEKDAYINT'] = pdMontrealDays['DATE'].apply(lambda x: x.weekday())
pdMontrealDays = pdMontrealDays.sort_values(by=['WEEKDAYINT'])
pdMontrealDays['WEEKDAY'] = pdMontrealDays['WEEKDAYINT'].apply(lambda x: cl.day_name[x])

#Creating new subset for visualizations by days of the week.
pdMontrealWeekDays = pdMontrealDays[['CATEGORY','WEEKDAY','DATE']]
pdMontrealWeekDays['COUNT']=pdMontrealWeekDays.groupby(['CATEGORY','WEEKDAY']).transform(np.size)
pdMontrealWeekDays.drop(['DATE'], axis=1, inplace=True)
pdMontrealWeekDays = pdMontrealWeekDays.drop_duplicates()

# COMMAND ----------

#Data Visualization
pdMontrealWeekDays=spark.createDataFrame(pdMontrealWeekDays)
display(pdMontrealWeekDays)

# COMMAND ----------

#Creating temporary/local PDQ table
spark.read.format("csv").option("header","true")\
  .option("inferSchema", "true")\
  .option("encoding","latin1")\
  .load("/FileStore/tables/pdq_point.csv")\
  .createOrReplaceTempView("PDQData")

# COMMAND ----------

#Converting the spark dataframe to Pandas to work on the column operations
pdq=spark.table("PDQData")
pdPDQData=pdq.toPandas()

# COMMAND ----------

#Data Cleaning

#Renaming columns
pdPDQData.rename(columns={"PREFIX_TEM":"ROAD","NOM_TEMP":"STREET"},inplace=True)

#Cleaning the dataframe to match the foreign key PDQNo from the Montreal table
pdPDQData[['temp','PDQNo']] = pdPDQData.DESC_LIEU.str.split('QUARTIER', expand = True)
pdPDQData['PDQ'] = pdPDQData['PDQNo'].astype(np.float64)
pdPDQData.drop(['temp','DESC_LIEU','NO_CIV_LIE','DIR_TEMP','MUN_TEMP','Longitude','Latitude'],axis =1 ,inplace=True)

# COMMAND ----------

#Data Transformation

pdMontrealRegion =pdMontreal[['PDQ','CATEGORY','DATE','QUART']]
pdPDQData = pdPDQData[['PDQ','ROAD','STREET']]
pd.to_numeric(pdPDQData['PDQ'])

cols =['PDQ']
pdMontrealRegion = pd.merge(pdMontrealRegion ,pdPDQData,on =cols)

#Creating dataset for achieving the region wise crime over years
pdMontrealRegionCrime=pdMontrealRegion[["CATEGORY","STREET","DATE"]]
pdMontrealRegionCrime["COUNT"]=pdMontrealRegionCrime.groupby(["CATEGORY","STREET"]).transform(np.size)
pdMontrealRegionCrime.drop(["DATE"],axis=1,inplace=True)
pdMontrealRegionCrime= pdMontrealRegionCrime.drop_duplicates()

# COMMAND ----------

#Data Visualization
pdMontrealRegionCrime=spark.createDataFrame(pdMontrealRegionCrime.tail(60))
display(pdMontrealRegionCrime)

# COMMAND ----------

#Data Transformation

pdMontrealRegionQuart=pdMontrealRegion[["CATEGORY","STREET","QUART"]]
pdMontrealRegionQuart["COUNT"]=pdMontrealRegionQuart.groupby(["STREET","QUART"]).transform(np.size)
pdMontrealRegionQuart.drop(["CATEGORY"],axis=1,inplace=True)
pdMontrealRegionQuart=pdMontrealRegionQuart.drop_duplicates()

# COMMAND ----------

#Data Visualization
pdMontrealRegionQuart=spark.createDataFrame(pdMontrealRegionQuart.head(30))
display(pdMontrealRegionQuart)

# COMMAND ----------

#Dataset for Season and Region wise crime
cols=['PDQ']
#pdMontrealMonthWiseSeason.drop("ROAD",axis =1 ,inplace=True);
#pdMontrealMonthWiseSeason.drop("STREET",axis =1 ,inplace=True);
pdMontrealMonthWiseSeason =pd.merge(pdMontrealMonthWiseSeason ,pdPDQData ,on=cols)
#pdMontrealMonthWiseSeason

pdMontrealRegionSeason=pdMontrealMonthWiseSeason[["CATEGORY","STREET","SEASON"]]
pdMontrealRegionSeason["COUNT"]=pdMontrealRegionSeason.groupby(["STREET","SEASON"]).transform(np.size)
pdMontrealRegionSeason.drop(["CATEGORY"],axis=1,inplace=True)
pdMontrealRegionSeason=pdMontrealRegionSeason.drop_duplicates()

# COMMAND ----------

#Data Visualization
pdMontrealRegionSeason=spark.createDataFrame(pdMontrealRegionSeason.tail(30))
display(pdMontrealRegionSeason)

# COMMAND ----------

from sklearn import metrics, linear_model
import matplotlib.pyplot as plt
import numpy as np
from sklearn import metrics
from sklearn.model_selection import train_test_split
import pandas as pd
from sklearn import metrics, linear_model
import datetime as dt
df = pd.read_csv("/dbfs/FileStore/tables/interventionscitoyendo.csv", encoding="latin1")
df['YEAR']=df.apply(lambda row : row['DATE'].split('-')[0],axis=1)
df['MONTH']=df.apply(lambda row : row['DATE'].split('-')[1],axis=1)
df['QUART']=df['QUART'].replace({'soir':3, 'jour':1 ,'nuit':2})
df["CATEGORY"]=df["CATEGORIE"].replace("Vol de véhicule à moteur",0)\
                                                .replace("Méfait",1)\
                                                .replace("Vols qualifiés",2)\
                                                .replace("Vol dans / sur véhicule à moteur",3)\
                                                .replace("Introduction",4)\
                                                .replace("Infractions entrainant la mort",5)

X=df[['YEAR','PDQ','QUART','MONTH']].fillna(0)
y=df["CATEGORIE"].fillna(0)
#y.unique()
print(X.shape)
print(y.shape)
#X = X[:len(X)-10] # to remove last 10 records
#print(X.shape)
#print(y.shape)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
random=linear_model.LogisticRegression()
y_predict =random.fit(X_train, y_train)
y_predict = random.predict(X_test)
metrics.accuracy_score(y_test, y_predict)#diff between actual and predicted output
