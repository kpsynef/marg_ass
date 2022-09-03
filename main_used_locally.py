#!/usr/bin/env python
# coding: utf-8

# In[2]:


import sqlalchemy
from fastapi import FastAPI,Response


# In[3]:


import pandas as pd


# In[4]:


dbengine=sqlalchemy.create_engine('sqlite:///C:\\sqlite\\margera_assignment.db')


# In[ ]:


app = FastAPI()

@app.get("/task_1")
def task1():
    query=pd.read_sql_query("SELECT Country,temp,day_name,max(validdate) FROM forecast GROUP BY Country,day_name ORDER BY Country,day",con=dbengine)

    return Response(query.to_json(orient="records"), media_type="application/json")


@app.get("/task_2")
def task2():
    query=pd.read_sql_query("""SELECT Country,day_name,ROUND(AVG(temp),1) as average_temp from
(SELECT Country,temp,day_name,validdate,row_number() OVER(PARTITION BY Country,day_name ORDER by validdate ) as row FROM forecast ORDER BY row DESC Limit 63  )
GROUP BY Country,day_name ORDER BY Country,day_name""",con=dbengine)


    return Response(query.to_json(orient="records"), media_type="application/json")

@app.get("/task_3")
def task3(n):
    query=pd.read_sql_query("""SELECT Country,MAX(temp) as temp,'max' as kpi_used FROM forecast 
GROUP BY Country
ORDER BY temp DESC
LIMIT %s""" %n ,con=dbengine)
    query2=pd.read_sql_query("""SELECT Country,MIN(temp) as temp,'min' as kpi_used FROM forecast 
GROUP BY Country
ORDER BY temp ASC
LIMIT %s""" %n ,con=dbengine)
    query=query.append(query2, ignore_index=True)

    query3=pd.read_sql_query("""SELECT Country, MAX(temp) as temp,kpi_used from
(SELECT Country,ROUND(AVG(temp),1) as temp,'max_avg' as kpi_used FROM forecast 
GROUP BY Country,day_name
ORDER BY temp DESC)
GROUP BY Country
ORDER BY temp DESC
LIMIT %s""" %n ,con=dbengine)
    query=query.append(query3,ignore_index=True)
    query4=pd.read_sql_query("""SELECT Country, MIN(temp) as temp,kpi_used from
(SELECT Country,ROUND(AVG(temp),1) as temp,'min_avg' as kpi_used FROM forecast 
GROUP BY Country,day_name
ORDER BY temp DESC)
GROUP BY Country
ORDER BY temp DESC
LIMIT %s""" %n ,con=dbengine)
    query=query.append(query4,ignore_index=True)
    


    return Response(query.to_json(orient="records"), media_type="application/json")

