# -*- coding: utf-8 -*-
"""
Created on Sun May  5 16:01:26 2019

Accident and Emergency Waiting Time by Hospital
Data Analysis Module

@author: Eric Fong

Python 3.7.3
"""

import requests
import json
import os
import pandas as pd
import urllib.request
import time
import datetime as dt
import sys
import re
import string
import numpy as np
##import matplotlib.pyplot as plt
##import seaborn as sns

from multiprocessing.pool import ThreadPool

## FUNCTIONS
## 1. RETRIEVE A LIST OF FILES TO BE DOWNLOADED FROM DATA.GOV.HK API
def retrieveFileList(start, end, url = "http://www.ha.org.hk/opendata/aed/aedwtdata-en.json"):
    params = {'url':url, 'start':start, 'end':end}    
    r = requests.get(url = 'https://api.data.gov.hk/v1/historical-archive/list-file-versions', params = params)
    return r

## 2. PARALLEL DOWNLOADING OF THE RAW DATA    
def job(input):
        urllib.request.urlretrieve("https://api.data.gov.hk/v1/historical-archive/get-file?url=http%3A%2F%2Fwww.ha.org.hk%2Fopendata%2Faed%2Faedwtdata-en.json&time=" + input, input + '.xml')

## 3. DOWNLOAD THE RAW DATA FROM DATA.GOV.HK BASED ON THE FILE LIST RETRIEVED
def downloadData(startStr, endStr):    
    ## RETRIEVE A LIST OF FILES TO BE DOWNLOADED FROM DATA.GOV.HK API
    fileListJSON = retrieveFileList(start = startStr, end = endStr).text
    
    ## EXTRACT THE TIMESTAMPS ELEMENTS
    timeStampList = pd.DataFrame({'ts':json.loads(fileListJSON)['timestamps']})
    
    ## PARALLEL DOWNLOAD
    ThreadPool(8).imap_unordered(job, timeStampList['ts'].values.tolist())
    
    
## 4. DOWNLOAD THE RAW DATA FROM DATA.GOV.HK BASED ON THE FILE LIST RETRIEVED
def downloadData2(fileList):    
    ## PARALLEL DOWNLOAD
    ##ThreadPool(8).imap_unordered(job, fileList)
    ThreadPool(8).map(job, fileList)
    


## CHANGE THE WORKING DIRECTORY
dirPath = 'D:\\OSC\\HA'
os.chdir(dirPath)

## CHECK IF THE COORDINATE FILE EXISTS
datFileExists = os.path.isfile('HospitalCoor.csv')
if datFileExists:
    hospCoor_df = pd.read_csv("HospitalCoor.csv", encoding = 'UTF-8')
else:
    sys.exit('Co-ordinate data file does not exist.')
    
## CAPTURE THE DATE OF TODAY
today_dt = dt.date.today()



############################# SECTION A #############################
######################## DOWNLOAD RAW DATA ##########################
 
## CHECK IF THE DATA FILE EXISTS
datFileExists = os.path.isfile('data.csv')

if datFileExists:
    ## DO STH
    print("DataFileExists")

## IF THE DATA FILE DOES NOT EXISTS    
else:
    ## RETRIEVE THE LIST OF FILES TO BE DOWNLOADED
    numDays = 14
    startDate = dt.datetime.strftime(today_dt + dt.timedelta(days=-numDays),'%Y%m%d')
    endDate = dt.datetime.strftime(today_dt + dt.timedelta(days=-1),'%Y%m%d')
    fileList = retrieveFileList(start = startDate, end = endDate)
    
    ## NO ERROR CODE, DATA OF YESTERDAY IS AVAILABLE
    if fileList:
        fileList = retrieveFileList(start = startDate, end = endDate).text
        
    ## ERROR CODE IN THE FIRST TRIAL, TRY TODAY()-2 DAYS
    else:
        startDate = dt.datetime.strftime(today_dt + dt.timedelta(days=-numDays-1),'%Y%m%d')
        endDate = dt.datetime.strftime(today_dt + dt.timedelta(days=-2),'%Y%m%d')
        fileList = retrieveFileList(start = startDate, end = endDate).text
        
        ## TERMINATE THE PROGRAM IF TODAY()-2 DAYS ALSO FAILS
        if "REQUEST ERROR" in fileList:
            sys.exit("DATA ERROR. PROGRAM IS TERMINATED.")
               
    ## EXTRACT THE LIST OF FILES TO BE DOWNLOADED FROM THE JSON OUTPUT
    fileList = json.loads(fileList)
    fileList = fileList['timestamps']

    ## LIST YYYYMMDD-HHmm.XML FILES IN THE WORKING DIRECTORY
    dirList = os.listdir()

    ## IDENTIFY THE "VALID" XML FILES
    r = re.compile("\d{8}-\d{4}\.xml")
    dirList = list(filter(r.match,dirList))
    dirList = [items[:13] for items in dirList]
    
    ## COMPARE THE TWO LISTS AND EXTRACT THOSE IN THE DATA.GOV>HK API OUTPUT BUT NOT IN THE WORKING DIRECTORY
    dlFileList = np.setdiff1d(fileList, dirList).tolist()
    
    ## DOWNLOAD THE FILES BASED ON dlFileList
    ## REPEAT UNTIL ALL FILES HAVE BEEN DOWNLOADED (CATER FOR OCCASIONAL CONNECTION FAILURES)
    while len(dlFileList)>0:
        downloadData2(dlFileList)
        
        ## RE-GENERATE dlFileList AFTER DOWNLOADING THE FILES
        dirList = os.listdir()
        r = re.compile("\d{8}-\d{4}\.xml")
        dirList = list(filter(r.match,dirList))
        dirList = [items[:13] for items in dirList]
        dlFileList = np.setdiff1d(fileList, dirList).tolist()
      
    ## IMPORT XML FILES IN THE WORKING DIRECTORY
    wt_list = []
    dirList = os.listdir()
    r = re.compile("\d{8}-\d{4}\.xml")
    dirList = list(filter(r.match,dirList))
    
    for fName in dirList:
        with open(fName) as json_file:
            json_data = json.load(json_file)
            json_dt = json_data['updateTime']
            json_data = json_data['waitTime']
            for item in json_data:
                json_temp = {'updateTime':json_dt, 'hospName':item['hospName'], 'topWait':item['topWait']}
                wt_list.append(json_temp)
                
    ## CREATE A PANDAS DF FOR FURTHER PROCESSING            
    waitTime_df = pd.DataFrame(wt_list)
    waitTime_df['updateTime'] = pd.to_datetime(waitTime_df['updateTime'], format="%d/%m/%Y %I:%M%p")
    waitTime_df['updateDate'] = waitTime_df['updateTime'].dt.date
    ##waitTime_df['updateWeek'] = waitTime_df['updateTime'].dt.week
    ##waitTime_df.dtypes
    
    ## VALUE MAPPING (WAIT TIME TO NUMERICAL)
    ## TO BE CONFIRMED
    val_map  = {"Around 1 hour" : 0.5, "Over 1 hour" : 1.5, "Over 2 hours" : 2.5,
             "Over 3 hours" : 3.5, "Over 4 hours":4.5, "Over 5 hours" : 5.5,
             "Over 6 hours" : 6.5, "Over 7 hours" : 7.5, "Over 8 hours" : 8.5}
    waitTime_df['topWait'] = waitTime_df['topWait'].map(val_map)

    ## WRITE TO A CSV DATA FILE
    waitTime_df.to_csv("AnE_Data.csv", index=False)


####################################### SECTION B #####################################
######################## ANALYZE THE DATA AND EXPORT TO JSON ##########################

## IMPORT THE RAW DATA FILE    
waitTime_df = pd.read_csv("AnE_Data.csv")

## FUNCTIONS
## 1. DAILY MEAN WAITING TIME BY HOSPITAL OF EACH DAY
def calc_meanWT (df):
    return df.groupby(['hospName','updateDate']).mean()

## 2. MEAN WAITING TIME BY HOSPITAL (SPEICFIC DATE RANGE, BOTH ENDS INCLUSIVE)
##    EACH DAY CARRIES EQUAL WEIGHT (REGARDLESS OF THE NUMBER OF RECORDS/DAY, SOMETIMES < 96)
##    RETURN ONLY ONE FIGURE FOR EACH OF THE HOSPITALS FOR THE DATE RANGE
def calc_meanWT_dateRng (df, start, end):
    temp_df = df[(df['updateDate']>=start) & (df['updateDate']<=end)].copy()
    return temp_df.groupby(['hospName']).mean()


## DAILY MEAN OF ALL DAYS
meanWT_all_df = calc_meanWT(waitTime_df)
meanWT_all_df.reset_index(inplace=True)
meanWT_all_df['updateDate'] = pd.to_datetime(meanWT_all_df['updateDate'])

## CALCULATE THE MEAN WAIT TIME (FROM TODAY()-7 to TODAY()-1)
past7_1_Day_meanWT_df = calc_meanWT_dateRng(meanWT_all_df, 
                                            pd.Timestamp(today_dt + dt.timedelta(days=-7)), 
                                            pd.Timestamp(today_dt + dt.timedelta(days=-1)))

## CALCULATE THE MEAN WAIT TIME (FROM TODAY()-14 to TODAY()-8)
past14_8_Day_meanWT_df = calc_meanWT_dateRng(meanWT_all_df,
                                             pd.Timestamp(today_dt + dt.timedelta(days=-14)), 
                                             pd.Timestamp(today_dt + dt.timedelta(days=-8)))

## CALCULATE THE CHANGE
out_meanWT_df = pd.merge(past14_8_Day_meanWT_df,past7_1_Day_meanWT_df,how='outer', left_index=True, right_index=True, suffixes=('_2w', '_1w'))
out_meanWT_df['PtgChange'] = (out_meanWT_df['topWait_1w']/out_meanWT_df['topWait_2w'])-1

## ADD BACK THE CHINESE NAME, LAT, LONG INFORMATION
out_meanWT_df = pd.merge(hospCoor_df, out_meanWT_df, how = 'inner', left_on = 'ENAME', right_index = True)
out_meanWT_df.rename(columns = {'topWait_1w':'WeekMeanTime'}, inplace = True)

## WRITE TO A JSON FILE
with open('output.json', 'w', encoding='utf-8') as file:
    out_meanWT_df.to_json(file,  force_ascii = False, orient='records')
                         
