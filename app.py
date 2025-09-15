import streamlit as st
import pandas as pd
import io
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import grouping_post as gp
import visualisation

import zipfile



from io import StringIO
import re
import os
import clipboard


def next_quote(df):
    print("next : ", st.session_state.count)
    if st.session_state.count + 1 >= len(df):
        st.session_state.count = 0
    else:
        st.session_state.count += 1

def previous_quote():
    print("previous : ", st.session_state.count)
    if st.session_state.count > 0:
        st.session_state.count = st.session_state.count - 1
    else:
        pass

def first_quote():
    print("previous : ", st.session_state.count)
    if st.session_state.count > 0:
        st.session_state.count = 0
    else:
        pass

def last_quote(df):
    print("next : ", st.session_state.count)
    if st.session_state.count + 1 >= len(df):
        st.session_state.count = 0
    else:
        st.session_state.count = len(df) - 1

def get_quote():
    quote =  st.session_state.stored_quote
    clipboard.copy(quote)
    st.toast(f"Copied to clipboard: {quote}")
        

def on_copy_click(text):
    clipboard.copy(text)
    st.toast(f"Copied to clipboard: {text}")


@st.dialog("Construisez votre requête")
def filter_builder(data):
    st.write("""On ne peut pas encore ajouter plusieurs filtre en même temps.<br>
    Si on veut uniquement les tweets de X publiés entre mars 2015 et mars 2016, on crée un premier filtre avec "Auteur" comme paramètre => on valide => on crée un nouveau filtre avec la date.""")
    filtre = st.selectbox("Select a column", [x for x in data.columns], index= None)

    if filtre:

        if data[f'{filtre}'].dtypes == "numeric" or data[f'{filtre}'].dtypes == "float":
            search_in_column = st.number_input("Insert a number")
            in_not_in = st.toggle("inferior") #do you look for tweets with the value or without the value
        else:
            if filtre == 'text':
                search_in_column = st.text_input("What are you looking for (if you are looking for several keywords, separate them by a comma (e.g. cancer, tumor))")
                key_search = search_in_column.replace("  ", " ").split(",")
                st.write("|".join(key_search).lower().replace(" |", " |").replace("| ", "|"))
                in_not_in = st.toggle("Looking for tweets without the value") #do you look for tweets with the value or without the value
            elif filtre == "local_time" or filtre == "date":
                start_date = st.date_input("Start of the period", value = "2010-01-01", min_value = "2010-01-01", max_value="2022-12-31")
                end_date = st.date_input("End of the period", value = "2022-12-31", min_value = "2010-01-01", max_value="2022-12-31")
                search_in_column = [start_date, end_date]
                in_not_in = False

            else:
                search_in_column = st.multiselect("Select a value", [x for x in data[f'{filtre}'].unique()])
                in_not_in = st.toggle("Looking for tweets without the value") #do you look for tweets with the value or without the value

        print(in_not_in)
        st.write(in_not_in)
    
        
        if st.button("Submit"):
            st.session_state.build_requete = {"column_name": filtre, "value": search_in_column, "in_not_in": in_not_in}
            st.rerun()





def topics(data,topic_column):
    d_topic = data.groupby([topic_column]).agg(nb = ("id", "size")).sort_values("nb", ascending=False).reset_index()
    list_topics = [x for x in d_topic[topic_column].unique()]
    return list_topics



def download_corpus(df):
    df1 = pd.DataFrame(df)
    print("OK", type(df1))
    components.html(
        download_button(object_to_download=df1, download_filename="corpus.csv"),
        height=0,
    )




@st.cache_data # évite que cette fonction soit exécutée à chaque fois
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv(index=False).encode('utf-8')



def rebase_count():
    st.session_state.count = 0



if 'count' not in st.session_state:
    st.session_state.count = 0



### Front hand

tab0, tab1 = st.tabs(["Tableau", "Texte par texte"])
with tab0:
    placeholder = st.empty()
    container = st.container()


with tab1:

    placeholder3 = st.empty()
    container3 = st.container()



#  Chargement du CSV contenant les tweets (un seul fichier à la fois)
st.session_state.dataframe = 0
st.session_state.corpus = False
st.sidebar.title("Mes données d'origine")


if 'filter_count' not in st.session_state:
    st.session_state.filter_count = -1



st.session_state.bdd = "CSV"
uploaded_files = st.sidebar.file_uploader("Téléverser le fichier ros1_tweets.csv", key = "df_tweet", accept_multiple_files=False)
uploaded_reply = st.sidebar.file_uploader("Téléverser le fichier join_reply.csv", key = "reply", accept_multiple_files=False)



if uploaded_files is not None:
    dic_id={}
    list_col = [x for x in pd.read_csv(uploaded_files).columns if 'id' in x]
    for x in list_col:
        dic_id[x] = "string"

    dic_id["text"] = "string"

    uploaded_files.seek(0)
    df0 = pd.read_csv(uploaded_files, dtype = dic_id)
    if uploaded_reply is not None:
        df_reply = pd.read_csv(uploaded_reply, sep=",", dtype =dic_id)

    

    st.sidebar.divider()
    df0 = df0.rename(columns={"user_screen_name":"author"})


  




    if st.sidebar.button("Create filter",  icon=":material/add:"):
        if st.session_state.filter_count == -1:
            filtre = filter_builder(data=df0)
            st.session_state.filter_count += 1
        else:
            filtre = filter_builder(data=st.session_state.data)
            st.session_state.filter_count += 1

    if st.sidebar.button("Reset filter", icon=":material/reset_settings:"):
        st.session_state.filter_count = -1

        del st.session_state["build_requete"]

        
    st.sidebar.write(st.session_state.filter_count)

    if "build_requete" in st.session_state:
        print("Filter Count : ", st.session_state.filter_count)
        if st.session_state.filter_count == 0:
            print("First filtre")
            st.sidebar.write(st.session_state.build_requete['column_name'], st.session_state.build_requete['value'])
            df = gp.df_processor(data=df0, column_name=st.session_state.build_requete['column_name'], value= st.session_state.build_requete['value'], in_not_in= st.session_state.build_requete['in_not_in'])
            st.session_state.data = df
        else:
            print("New filtre")
            st.sidebar.write(st.session_state.build_requete['column_name'], st.session_state.build_requete['value'])
            df = gp.df_processor(data=st.session_state.data, column_name=st.session_state.build_requete['column_name'], value= st.session_state.build_requete['value'], in_not_in= st.session_state.build_requete['in_not_in'])
            st.session_state.data = df


    else:
        df = gp.df_processor(data=df0, column_name=None, value= None, in_not_in=None)

   

    st.session_state.dataframe = 1





if st.session_state.dataframe == 1:


   
    with tab0 :
        #st.divider()
        placeholder = st.empty()
        container = st.container()

        with placeholder.container():
            st.write("Nombre de textes: ", len(df))
            print(df.columns)
            visualisation.display_dataframe(data=df)
            fig = visualisation.tracer_graphique(data=df, d = "Jour")
            timeserie = st.pyplot(fig)


    with tab1 :
        placeholder3 = st.empty()
        container3 = st.container()
        with placeholder3.container():
            #show_text = visualisation.display_text(data=df)
            visualisation.display_quote1(df, df_reply)
            col1, col2, col3, col4, col5 = st.columns(5)

            with col1:
                if st.button("⏮️ ⏮️ First"):
                    first_quote()
                else:
                    pass

            with col2:
                if st.button("⏮️ Previous"):
                    previous_quote()
                else:
                    pass

            with col3:
                if st.button("Copy quote 📋"):
                    get_quote()
                else:
                    pass
                

            with col4:
                if st.button("Next ⏭️"):
                    next_quote(df)
                else:
                    pass

            with col5:
                if st.button("Last ⏭️ ⏭️"):
                    last_quote(df)
                else:
                    pass



elif st.session_state.dataframe == 0 :
    with tab0 :
        placeholder = st.empty()
        container = st.container()
        with placeholder.container():
            st.header("Aucun CSV")
            st.markdown(
                """
                Si vous voulez construire un corpus pour Prospéro, chargez un fichier ou connectez-vous à la base de données.
                Le CSV doit avoir les 3 colonnes suivantes (l'ordre n'a pas d'importance):

                |Author | Text | Date |
                |-------|------|------|
                | X     | lllll| YYYY-MM-DD|
                """
            )

    with tab1:

        placeholder3 = st.empty()
        container3 = st.container()
        with placeholder3.container():
            st.header("Read text")
            st.markdown(
                """
                No texts
                """
            )

elif st.session_state.dataframe == -1 :
    with tab0 :
        placeholder = st.empty()
        container = st.container()
        with placeholder.container():
            st.header("0 publication")
            st.markdown(
                """
                Aucune publication ne correspond aux éléments de la requête. Essayez une autre émission ou d'autre.s candidat.es
                """
            )

    with tab0:

        placeholder3 = st.empty()
        container3 = st.container()
        with placeholder3.container():
            st.header("0 publication")
            st.markdown(
                """
                Aucune publication ne correspond aux éléments de la requête. Essayez une autre émission ou d'autre.s candidat.es
                """
            )
