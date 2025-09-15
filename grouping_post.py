import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import numpy as np

@st.cache_data
def df_processor(data, column_name, value, in_not_in):
    '''Fonction pour mettre en forme le fichier csv : on définit le type des colonnes, on convertit les colonnes "de date" au format Date etc.

    uploaded_files : a csv'''
    if "local_time" in data.columns:
        data['local_time'] = pd.to_datetime(pd.to_datetime(data['local_time']))
    elif "pub_date" in data.columns:
        data['local_time'] = pd.to_datetime(pd.to_datetime(data['pub_date']))
    elif "date" in data.columns:
        data['local_time'] = pd.to_datetime(pd.to_datetime(data['date']))


    data['date'] = pd.to_datetime(pd.to_datetime(data['local_time']).dt.date)

    data['yearmonth']=(data['date'].dt.strftime('%Y-%m'))
    data["yearmonth"] = pd.to_datetime(data.yearmonth, format='%Y-%m')

    if 'text' in data.columns:
    #On compte le nombre de mot pour ensuite filtrer les texts en fonction de leur longueur
        data["length_text"] = data.text.str.len()
        data["split_txt"] = data.text.str.split(" ")
        data["nb_word"]= data.split_txt.str.len()
    elif 'text_content' in data.columns:
        data = data.rename(columns={"text_content": "text"})
        data["length_text"] = data.text.str.len()
        data["split_txt"] = data.text.str.split(" ")
        data["nb_word"]= data.split_txt.str.len()
    elif 'content' in data.columns:
        data = data.rename(columns={"content": "text"})
        data["length_text"] = data.text.str.len()
        data["split_txt"] = data.text.str.split(" ")
        data["nb_word"]= data.split_txt.str.len()





    data["year"]= data.local_time.dt.year
    data["month"]= data.local_time.dt.month
    data["day"]= data.local_time.dt.day
    data["hour"]= data.local_time.dt.time

    if column_name is None:
        data = data.sort_values("local_time",ascending=True).reset_index().drop(columns=["index"])
    else:
        if data[column_name].dtypes == "float":
            data = data.loc[data[column_name] >= value].sort_values("local_time",ascending=True).reset_index().drop(columns=["index"])
        elif data[column_name].dtypes == "datetime64[ns]":
            start_date  =  np.datetime64(value[0])
            end_date  =  np.datetime64(value[1])
            data = data.loc[(data[column_name] >= start_date) & (data[column_name] <= end_date)].sort_values("local_time",ascending=True).reset_index().drop(columns=["index"])
            #data = data.sort_values("local_time",ascending=True).reset_index().drop(columns=["index"])

        else:
            if column_name == "text":
                if in_not_in==False:
                    data = data.loc[data[column_name].str.lower().str.contains(value)].sort_values("local_time",ascending=True).reset_index().drop(columns=["index"])
                else:
                    data = data.loc[~data[column_name].str.lower().str.contains(value)].sort_values("local_time",ascending=True).reset_index().drop(columns=["index"])
            else:
                if in_not_in==False:
                    data = data.loc[data[column_name].isin(value)].sort_values("local_time",ascending=True).reset_index().drop(columns=["index"])
                else:
                    data = data.loc[~data[column_name].isin(value)].sort_values("local_time",ascending=True).reset_index().drop(columns=["index"])




    return data

def group_by_user_by_minute(data, number, minute_interval):

    data = data.loc[data["nb_word"]>=number]
    list_user =[]
    list_text=[]
    list_min_date=[]
    list_max_date=[]
    list_local_time = []
    for n, user in enumerate(data.author.unique()):
        print(user)
        dtemp = data.loc[data["author"]==user]
        dtemp = dtemp.drop_duplicates(subset="text")
        n_row = len(dtemp)
        compteur = 0
        while compteur < n_row:
            first_tweet = dtemp.local_time.min()
            dtemp1 = dtemp.loc[(dtemp["local_time"] >= first_tweet) &
                               (dtemp["local_time"]<= first_tweet+timedelta(minutes = int(minute_interval)))]
            min_date = dtemp1.local_time.min()
            max_date = dtemp1.local_time.max()
            for m, tweets in enumerate(dtemp1.text):
                if m == 0:
                    concat_text = tweets
                else:
                    concat_text = f"{concat_text}\n.\n\n{tweets}"
            concat_text = concat_text.replace('«\xa0', '\"')
            concat_text = concat_text.replace('\xa0»', '\"')
            concat_text = concat_text.replace("’", "\'")
            list_user.append(user)
            list_text.append(concat_text)
            list_min_date.append(min_date)
            list_max_date.append(max_date)
            dtemp = dtemp.loc[(dtemp["local_time"]>= first_tweet+timedelta(minutes = int(minute_interval)))]
            

            compteur += len(dtemp1)

    dict_data = {"author": list_user, "text":list_text, "min_date": list_min_date, "max_date":list_max_date}
    dg = pd.DataFrame(dict_data)
    dg["year"]= dg.min_date.dt.year
    dg["month"]= dg.min_date.dt.month
    dg["day"]= dg.min_date.dt.day
    dg["hour"]= dg.min_date.dt.time
    dg["date"] = dg.min_date.dt.date
    dg["source"] = "Twitter"
    dg = dg.merge(data[["author"]].drop_duplicates(), on = ["author"], how = "left")

    return dg
