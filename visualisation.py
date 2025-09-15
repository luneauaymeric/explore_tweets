import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

@st.cache_data
def display_text(data):
    for n, x in enumerate(data.text):
        if 'user_screen_name' in data.columns:
            st.write('__Name:__ ', data['user_screen_name'].iloc[n])
        elif 'author' in data.columns:
            st.write('__Name:__ ', data['author'].iloc[n])

        if "User_status" in data.columns:
            st.write('__Statut:__ ', data['User_status'].iloc[n])
        else:
            pass
        #st.write(x)
        if "reply_to" in data.columns:
            if data.retweeted_id.isnull().iloc[n] == False:
                st.write('__retweet de :__ ', data['retweeted_user_id'].iloc[n])
            else:
                pass
        else:
            pass
        if "local_time" in data.columns:
            st.write('__Date:__ ', data['local_time'].iloc[n])
        else:
            pass

        st.write('__Emission:__', data['Publication Title'].iloc[n])
        st.write('__Candidat.e:__', data['Guest'].iloc[n])
        #st.write('Organ: ', tweets['Organ'].iloc[n])
        st.write(x)
        st.divider()


# Fonction pour tracer la série temporelle
@st.cache_data
def tracer_graphique(data, d):
    scale = {"Année":"y","Mois":"m","Jour":"d"}
    df1 = data.groupby(["local_time"]).agg(nb=('id','size')).reset_index()
    #df["num"] = 1 # valeur par article pour comptage
    leg = []
    fig,ax=plt.subplots(1,figsize=(10,3))
    if "User_status" in data.columns:
        for i, j in data.groupby("User_status"):
            leg.append(i)
            j.set_index("date")["id"].resample(scale[d]).size().plot(ax=ax,style=".-")
        plt.legend(leg)
    else:
        data.set_index("local_time")["id"].resample(scale[d]).size().plot(ax=ax,style=".-")
        #plt.plot(df1.local_time, df1.nb)
    plt.title("Distribution temporelle des publications")
    plt.xlabel("Date (par %s)"%d)
    plt.ylabel("Nombre de poste")
    plt.tight_layout()
    return fig

@st.cache_data
def display_dataframe(data):
    try:
        data = data[["author", "text", "local_time", "source","Publication Title","Guest"]].sort_values("local_time",ascending=True).reset_index().drop(columns=["index"])
    except:
        
        data = data[["author", "User_status", "text", "local_time", "retweeted_id", 'retweeted_user_id']].sort_values("local_time",ascending=True).reset_index().drop(columns=["index"])
    st.dataframe(data)


def display_quote1(data, dreply):
    index = st.session_state.count
    print(index)
    print("Data columns : ", data.columns)

    
    if 'user_screen_name' in data.columns:
        auteur = data['user_screen_name'].iloc[index]
        st.write('__Name:__ ', auteur)
    elif 'author' in data.columns:
        auteur = data['author'].iloc[index]
        st.write('__Name:__ ', auteur)

    if "User_status" in data.columns:
        statut = data['User_status'].iloc[index]
        st.write('__Statut:__ ', statut)
    else:
        pass
    #st.write(x)
    if "reply_to" in data.columns:
        if data.retweeted_id.isnull().iloc[n] == False:
            st.write('__retweet de :__ ', data['retweeted_user_id'].iloc[index])
        else:
            pass
    else:
        pass
    if "local_time" in data.columns:
        date_pub = data['local_time'].iloc[index]
        st.write('__Date:__ ', date_pub)
    else:
        pass
    #st.write('Organ: ', tweets['Organ'].iloc[n])

    if 'id' in data.columns:
        st.write('__Tweet id:__ ', data['id'].iloc[index])
    else:
        pass

    if "Publication Title" in data.columns:
        st.write('__Emission:__', data['Publication Title'].iloc[index])
    else:
        pass
    if "Guest" in data.columns:
        st.write('__Candidat.e:__', data['Guest'].iloc[index])
    else:
        pass
    if "hashtags" in data.columns:
        st.write('__Hashtags:__', data['hashtags'].iloc[index])
    else:
        pass

    if "group" in data.columns:
        topic_name = data['group'].iloc[index]
        st.write('__Synthetic topic:__', topic_name)
    else:
        pass

    df_retweet = data.loc[data.retweeted_id==data.id.iloc[index]]
    st.write('__Nombre de retweets:__', len(df_retweet))  
    
    quote_txt = data.text.iloc[index]
    st.write(quote_txt)

    date_date = data["date"].dt.date.iloc[index]
    st.session_state.stored_quote = f"\"{quote_txt}\", ({statut}, {topic_name}, {date_date})"
    

    try:
        if data.reply_id.isna().iloc[index] == False:
            st.divider()
            
            rep_id = data.reply_id.iloc[index]
            nom = dreply.user_screen_name.loc[dreply.id == rep_id].iloc[0]
            st.write(f"__Le tweet de {auteur} est une réponse à")
            st.write('__Reply to__ : ', dreply.user_screen_name.loc[dreply.id == rep_id].iloc[0])
            st.write('__Reply id__ : ', dreply.id.loc[dreply.id == rep_id].iloc[0])
            #st.write('__Reply status__ : ', data.User_status.loc[data.author == nom].iloc[0])
            st.write('__Date__ : ', dreply.local_time.loc[dreply.id == rep_id].iloc[0])
            st.write('__text__ : ', dreply.text.loc[dreply.id == rep_id].iloc[0])
        else:
            pass

    except:
        pass

    st.divider()
    st.header("Listes des retweets")

    if data.retweeted_id.isna().iloc[index] == True:
        
        st.dataframe(df_retweet[["author", "text", "local_time", "retweeted_id"]])
    else:
        df_original_tweet = data.loc[data.id == data.retweeted_id.iloc[index]]
        st.dataframe(df_original_tweet[["author", "text", "local_time", "retweeted_id"]])

    st.divider()
