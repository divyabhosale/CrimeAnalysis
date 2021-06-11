# Databricks notebook source
#Initial Setup
import numpy as np
import pandas as pd
from datetime import datetime as dt
import calendar as cl
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# COMMAND ----------

#Creating temporary/local table
spark.read.format("csv").option("header","true").option("inferSchema", "true").load("/FileStore/tables/NYPD_Complaint_Data_Historic__version_1__xlsb-0a052.csv").createOrReplaceTempView("NewYorkCrimeData")

# COMMAND ----------

#Converting the spark dataframe to Pandas to work on the column operations
newYork=spark.table("NewYorkCrimeData")
pdNewYork=newYork.toPandas()

# COMMAND ----------

#Creating temporary/local table
spark.read.format("csv").option("header","true").option("inferSchema", "true").load("/FileStore/tables/NYPD2_Complaint_Data_Current__Year_To_Date2_.csv").createOrReplaceTempView("NewYorkCrimeDataCurrentYear")

# COMMAND ----------

#Converting the spark dataframe to Pandas to work on the column operations
newYorkCurrentYears=spark.table("NewYorkCrimeDataCurrentYear")
pdNewYorkCurrentYears=newYorkCurrentYears.toPandas()

# COMMAND ----------

#Merging the datasets
pdNewYorkWhole = pd.concat([pdNewYork, pdNewYorkCurrentYears])

# COMMAND ----------

#Renaming the columns
pdNewYorkWhole= pdNewYorkWhole.rename(columns={"CMPLNT_NUM":"COMPLAINTNO", "CMPLNT_FR_DT":"COMPLAINTDATE", "CMPLNT_FR_TM":"COMPLAINTTIME", "CMPLNT_TO_DT" : "COMPLAINTENDDATE", "CMPLNT_TO_TM":"COMPLAINTENDTIME", "RPT_DT":"REPORTDATE","KY_CD":"OFFENCEKEY","OFNS_DESC":"OFFENCE", "PD_CD":"CLASSCODE", "PD_DESC":"CLASSCODEDESC", "LAW_CAT_CD":"LEVELOFOFFENCE","CRM_ATPT_CPTD_CD":"CRIMECASESTATUS", "JURIS_DESC":"JURISDICTION", "BORO_NM":"AREACODE", "ADDR_PCT_CD":"ADDSPCT", "LOC_OF_OCCUR_DESC":"LOCATIONOFOFFENCE", "PREM_TYP_DESC":"NYCPREMISES", "PARKS_NM":"NYCPARK", "HADEVELOP":"NYCHOUSING","X_COORD_CD":"XCOORDINATE", "Y_COORD_CD":"YCOORDINATE", "Lat_Lon":"LATLON"})

# COMMAND ----------

#Data Processing
#Creating a super category of the given offences
temp=pdNewYorkWhole.OFFENCE.fillna("0")
pdNewYorkWhole["CATEGORY"] = pd.np.where(temp.str.contains("SUBSTANCE") | temp.str.contains("DRUGS") | temp.str.contains("MARIJUANA") ,"POSSESSION OF DRUGS",
                   pd.np.where(temp.str.contains("ASSAULT"), "ASSAULT",
                   pd.np.where(temp.str.contains("FORGERY") | temp.str.contains("FRAUD"), "FRAUD",
                   pd.np.where(temp.str.contains("LARCENY"), "LARCENY",
                   pd.np.where(temp.str.contains("LAW"), "LAW BREAKING",
                   pd.np.where(temp.str.contains("WEAPONS"), "WEAPONS POSSESSION",
                   pd.np.where(temp.str.contains("SEX") | temp.str.contains("SEXUAL") | temp.str.contains("HARASSMENT") | temp.str.contains("PROSTITUTION") | temp.str.contains("RAPE") , "SEXUAL ABUSE",
                   pd.np.where(temp.str.contains("ROBBERY") | temp.str.contains("THEFT") | temp.str.contains("BURGLARY") | temp.str.contains("ARSON") , "ROBBERY","OTHERS"))))))))

# COMMAND ----------

#Extracting Year from Date
pdNewYorkWhole['YEAR'] = pdNewYorkWhole['COMPLAINTDATE'].str.slice(start=-4)

#Data Processing
pdNewYorkYearWise= pdNewYorkWhole[['YEAR','CATEGORY']]

#Removing the NaN values
pdNewYorkYearWise= pdNewYorkYearWise.dropna(how='all')

#Forming a new column with count of category for given years
pdNewYorkYearWise['COUNT']=pdNewYorkYearWise.groupby(['YEAR','CATEGORY'])['CATEGORY'].transform('size')
pdNewYorkYearWise=pdNewYorkYearWise.drop_duplicates()
pdNewYorkYearWise['YEAR']=pdNewYorkYearWise['YEAR'].apply(pd.to_numeric)
pdNewYorkYearWise=pdNewYorkYearWise.sort_values('YEAR')
pdNewYorkYearWise=pdNewYorkYearWise[pdNewYorkYearWise['YEAR'].isin([2013,2014,2015,2019])]
pdNewYorkYearWise=pdNewYorkYearWise.sort_values('YEAR')

# COMMAND ----------

#Data Visualization
pdNewYorkYearWiseVisual=spark.createDataFrame(pdNewYorkYearWise)
display(pdNewYorkYearWiseVisual)

# COMMAND ----------

#Data Processing
#Extract Time from Complaint Time
pdNewYorkWhole['TIME'] = pdNewYorkWhole['COMPLAINTTIME'].str.split(':').str[0]
pdNewYorkWhole['TIME']=pdNewYorkWhole['TIME'].apply(pd.to_numeric)
result = [] 
for value in pdNewYorkWhole['TIME']: 
    if value >= 6 and value <= 16: 
        result.append("Day") 
    elif value >= 17 and value <= 20: 
        result.append("Evening") 
    else:
        result.append("Night") 
       
pdNewYorkWhole['TIMEOFDAY'] = result 

#Forming a new column with count of category for unique results
pdNewYorkTotalCrimeTime= pdNewYorkWhole[['TIMEOFDAY','CATEGORY']]
pdNewYorkTotalCrimeTime['COUNT']=pdNewYorkTotalCrimeTime.groupby(['TIMEOFDAY','CATEGORY'])['CATEGORY'].transform('size')
pdNewYorkTotalCrimeTime=pdNewYorkTotalCrimeTime.drop_duplicates()


# COMMAND ----------

#Data Visualization
pdNewYorkTotalCrimeTimeVisual=spark.createDataFrame(pdNewYorkTotalCrimeTime)
display(pdNewYorkTotalCrimeTimeVisual)

# COMMAND ----------

#Data Processing
#Extract Time from Complaint Time
pdNewYorkWhole['TIME'] = pdNewYorkWhole['COMPLAINTTIME'].str.split(':').str[0]
pdNewYorkWhole['TIME']=pdNewYorkWhole['TIME'].apply(pd.to_numeric)
result = [] 
for value in pdNewYorkWhole['TIME']: 
    if value >= 6 and value <= 16: 
        result.append("Day") 
    elif value >= 17 and value <= 20: 
        result.append("Evening") 
    else:
        result.append("Night") 
       
pdNewYorkWhole['TIMEOFDAY'] = result 

#Forming a new column with count of category for unique results
pdNewYorkTotalCrimeTime= pdNewYorkWhole[['TIMEOFDAY','CATEGORY']]
pdNewYorkTotalCrimeTime['COUNT']=pdNewYorkTotalCrimeTime.groupby(['TIMEOFDAY','CATEGORY'])['CATEGORY'].transform('size')
pdNewYorkTotalCrimeTime=pdNewYorkTotalCrimeTime.drop_duplicates()


# COMMAND ----------

#Data Visualization
pdNewYorkTotalCrimeTimeVisual=spark.createDataFrame(pdNewYorkTotalCrimeTime)
display(pdNewYorkTotalCrimeTimeVisual)

# COMMAND ----------

#Data Processing

#Extract month from date
pdNewYorkWhole['MONTH']=pdNewYorkWhole['COMPLAINTDATE'].str.slice(stop=2)
pdNewYorkWhole['MONTH'] = pdNewYorkWhole['MONTH'].str.split('/').str[0]
pdNewYorkWhole['MONTH']=pdNewYorkWhole['MONTH'].apply(pd.to_numeric)
result = [] 
for value in pdNewYorkWhole['MONTH']: 
    if value >= 4 and value <= 7: 
        result.append("Summer") 
    elif value >= 7 and value <= 10: 
        result.append("Fall") 
    else:
        result.append("Winter") 
       
pdNewYorkWhole['SEASON'] = result

#Get the count of category for the particular season
pdNewYorkSeasonWise =pdNewYorkWhole[['CATEGORY','SEASON']]
pdNewYorkSeasonWise['COUNT']=pdNewYorkSeasonWise.groupby(['SEASON','CATEGORY'])['CATEGORY'].transform('size')
pdNewYorkSeasonWise=pdNewYorkSeasonWise.drop_duplicates()

# COMMAND ----------

#Data Visaualization
pdNewYorkSeasonWiseVisual=spark.createDataFrame(pdNewYorkSeasonWise)
display(pdNewYorkSeasonWiseVisual)

# COMMAND ----------

#Data Processing
#Get location wise data
pdNewYorkAreaWise= pdNewYorkWhole[['CATEGORY','AREACODE']]
pdNewYorkAreaWise=pdNewYorkAreaWise.dropna()
pdNewYorkAreaWise['COUNT']=pdNewYorkAreaWise.groupby(['AREACODE','CATEGORY'])['AREACODE'].transform('size')
pdNewYorkAreaWise=pdNewYorkAreaWise.drop_duplicates()

# COMMAND ----------

#Data Visaualization
pdNewYorkAreaWiseVisual=spark.createDataFrame(pdNewYorkAreaWise)
display(pdNewYorkAreaWiseVisual)

# COMMAND ----------

#Data Transformation

pdNewYorkAreaTimeOfDay= pdNewYorkWhole[["CATEGORY","AREACODE","TIMEOFDAY"]]
pdNewYorkAreaTimeOfDay=pdNewYorkAreaTimeOfDay.dropna()
pdNewYorkAreaTimeOfDay["COUNT"]=pdNewYorkAreaTimeOfDay.groupby(["AREACODE","TIMEOFDAY"]).transform(np.size)
pdNewYorkAreaTimeOfDay.drop(['CATEGORY'], axis=1, inplace=True)
pdNewYorkAreaTimeOfDay=pdNewYorkAreaTimeOfDay.drop_duplicates()

# COMMAND ----------

#Data Visaualization
pdNewYorkAreaTimeOfDay=spark.createDataFrame(pdNewYorkAreaTimeOfDay)
display(pdNewYorkAreaTimeOfDay)

# COMMAND ----------

#Data Processing
#Get location wise data
pdNewYorkAreaSeasonWise= pdNewYorkWhole[['SEASON','AREACODE']]
pdNewYorkAreaSeasonWise=pdNewYorkAreaSeasonWise.dropna()
pdNewYorkAreaSeasonWise['COUNT']=pdNewYorkAreaSeasonWise.groupby(['AREACODE','SEASON'])['AREACODE'].transform('size')
pdNewYorkAreaSeasonWise=pdNewYorkAreaSeasonWise.drop_duplicates()

# COMMAND ----------

#Data Visualization
pdNewYorkAreaSeasonWiseVisual=spark.createDataFrame(pdNewYorkAreaSeasonWise)
display(pdNewYorkAreaSeasonWiseVisual)
