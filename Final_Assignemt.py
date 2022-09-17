#!/usr/bin/env python
# coding: utf-8

# In[17]:


import glob                         
import pandas as pd                 
import xml.etree.ElementTree as ET  
from datetime import datetime


# In[27]:


tmpfile    = "temp.tmp"               
logfile    = "logfile.txt"            
targetfile = "bank_market_cap_1.json"    


# In[28]:

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe


# In[29]:


columns=['Name','Market Cap (US$ Billion)']


# In[30]:

def extract():
   
    extracted_data = pd.DataFrame(columns=['Name','Market Cap (US$ Billion)'])
    
   
    for jsonfile in glob.glob("bank_market_cap_1.json"):
        extracted_data = extracted_data.append(extract_from_json(jsonfile), ignore_index=True)
        
    return extracted_data


# In[31]:




extracted_data = extract()

extracted_data.head()


# In[35]:



def transform(data):
    # Write your code here
    data['Market Cap (US$ Billion)'] = round(0.732 * data['Market Cap (US$ Billion)'], 3)
    data.rename(columns={'Market Cap (US$ Billion)': 'Market Cap (GBP$ Billion)'}, inplace=True)
    return data

transformed_data = transform(extracted_data)
transformed_data.head


# In[36]:


def load(target_file, data_to_load):

    data_to_load.to_csv(target_file, index=False) 


# In[37]:



def log(message):

    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')


# In[38]:


log("ETL Job Started")
log("Extract phase Started")
extracted_data = extract()
extracted_data.head()
log("Extract phase Ended")
log("Transform phase Started")
transformed_data = transform(extracted_data)
transformed_data.head()
log("Transform phase Ended")
log("Load phase Started")
load(targetfile,transformed_data)
log("Load phase Ended")
log("ETL Job Ended")





