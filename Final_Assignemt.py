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
targetfile = "exchange_rates.csv"   


# In[28]:


df = pd.read_csv("exchange_rates.csv")


# In[29]:


df


# In[30]:


exchange_rate = df._get_value(9,'Rates')


# In[31]:


exchange_rate


# In[35]:


def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe


# In[36]:


columns=['Rates','Currencey']


# In[37]:


def extract():
    extracted_data = pd.DataFrame(columns=['Rates'])
    
    #process all csv files
    for csvfile in glob.glob("exchange_rates.csv"):
        extracted_data = extracted_data.append(extract_from_csv(csvfile), ignore_index=True)
        return extracted_data


# In[38]:


extracted_data = extract()
extracted_data.head()


# In[39]:


def transform(data):
    # Write your code here
    data['Rates'] = round(0.75 * data['Rates'], 3)
    return data


# In[40]:


Transformed_data = transform(extracted_data)
Transformed_data.head()


# In[41]:


def load(target_file, data_to_load):
    # Write your code here
    data_to_load.to_csv(target_file, index=False) 


# In[42]:


def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')


# In[43]:


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


# In[ ]:




