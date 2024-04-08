import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import sys
import os
import pyarrow.parquet as pq
import requests
from tempfile import NamedTemporaryFile
import json
from io import BytesIO

# Dynamically add the project root to sys.path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from dags.trigger_dag import trigger_dag
from streamlit import session_state as state

st.set_option('deprecation.showPyplotGlobalUse', False)


def generate_random_data(num_points):
    return np.random.choice([0, 1], size=num_points)

def generate_wordcloud(text):
    wordcloud = WordCloud(width=800, height=400, max_words=200, background_color='white').generate(text)
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    st.pyplot()

# Function to setup the sidebar search and filters
def setup_sidebar(webscrap):
    st.sidebar.title("Recherche")
    search_query = st.sidebar.text_input("Produit, catégorie ou sous-catégorie :", "")

    st.sidebar.title("Filtres")
    selected_category = st.sidebar.selectbox("Catégorie", webscrap['Catégorie'].unique())
    selected_subcategory = st.sidebar.selectbox("Sous-Catégorie", webscrap[webscrap['Catégorie'] == selected_category]['Sous-Catégorie'].unique())
    selected_product = st.sidebar.selectbox("Produit", webscrap[webscrap['Sous-Catégorie'] == selected_subcategory]['Produit'].unique())

    
    # Store selected_product in Streamlit's state to keep it between reruns
    state.selected_product = selected_product
    
    return search_query, selected_category, selected_subcategory, selected_product


# Function to filter data based on sidebar selections
def filter_data(search_query, selected_category, selected_subcategory, selected_product, webscrap, data):
    if not search_query:
        filtered_data = webscrap[
            (webscrap['Catégorie'] == selected_category) &
            (webscrap['Sous-Catégorie'] == selected_subcategory) &
            (webscrap['Produit'] == selected_product)
        ]
    else:
        filtered_data = data
    return filtered_data


# Function to display the main content of the app
def display_content(selected_product, df, countries):
    st.title(f"{selected_product} Reputation")

    row1 = st.columns(3)
    row2 = st.columns(2)


    with row2[0]:
        selected_country = st.selectbox("Sélectionnez un pays", countries)
        num = df[selected_country].value_counts()
        colors = {0: 'red', 1: 'green'}
        fig_donut = px.pie(df, names=selected_country, color=selected_country, hole=0.4,color_discrete_map=colors, title='Diagramme en secteurs')
        st.plotly_chart(fig_donut, use_container_width=True)

    with row1[0]:
        container = st.container()
        container.markdown("**Commentaire Total**")
        container.write(len(df))

    with row1[1]:
        container = st.container()
        container.markdown("**Commentaires positifs**")
        container.write(num.get(1, 0))

    with row1[2]:
        container = st.container()
        container.markdown("**Commentaires négatifs**")
        container.write(num.get(0, 0))

    with row2[1]:
        coordinates = [[34.02, -6.83], [48.86, 2.35], [35.68, 139.76], [40.71, -74.01], [-26.2, 28.04]]
        countries = ['Morocco','France','Japan','USA','South Africa']

        df = pd.DataFrame(np.concatenate([coordinates, np.random.randint(10, 30, (5, 2))], axis=1),
                          columns=['lat', 'lon', 'Negative', 'Positive'])
        df['Country'] = countries

        # Color markers based on the comparison of 'Positive' and 'Negative'
        df['Color'] = np.where(df['Positive'] > df['Negative'], '#00ff00', '#ff0000')
        # Create a Streamlit map
        st.map(df, color="Color", size = 500000)


def read_hdfs_parquet_file(product_name):
    hdfs_path = f'hdfs://namenode:8020/hadoop/hdfs/youtube/{product_name}.parquet'
    table = pq.read_table(hdfs_path)
    df = table.to_pandas()
    return df


def read_hdfs_file_via_webhdfs_stream(product_name):
    webhdfs_url = f'http://namenode:50070/webhdfs/v1/hadoop/hdfs/youtube/{product_name}.parquet?op=OPEN'
    # Use stream=True to fetch the content in chunks
    with requests.get(webhdfs_url, stream=True) as response:
        response.raise_for_status()  # Ensure the request was successful
        with NamedTemporaryFile(delete=True) as tmp_file:
            # Stream the content into a temporary file
            for chunk in response.iter_content(chunk_size=8192): 
                tmp_file.write(chunk)
            tmp_file.seek(0)  # Go back to the beginning of the temporary file

            # Now read the temporary Parquet file with PyArrow
            table = pq.read_table(tmp_file.name, columns=['sentiment'])
            df = table.to_pandas()
            return df

def read_parquet_file_via_webhdfs(full_file_path):
    open_url = f"http://localhost:50071/webhdfs/v1{full_file_path}?op=OPEN"
    response = requests.get(open_url, allow_redirects=True)  # Follow redirects to DataNode
    if response.status_code == 200:
        # Assuming the data is small enough to fit into memory comfortably
        file_content = BytesIO(response.content)
        table = pq.read_table(file_content)
        return table.to_pandas()
    else:
        raise Exception(f"Failed to read file. HTTP status code: {response.status_code}")


def list_hdfs_directory_files(product_name):
    list_url = f"http://localhost:50071/webhdfs/v1/hadoop/hdfs/youtube/{product_name}.parquet?op=LISTSTATUS"
    response = requests.get(list_url)
    if response.status_code == 200:
        file_statuses = response.json().get("FileStatuses", {}).get("FileStatus", [])
        file_names = [file_status["pathSuffix"] for file_status in file_statuses if file_status["type"] == "FILE"]
        return file_names
    else:
        #raise Exception(f"Failed to list directory. HTTP status code: {response.status_code}")
        return None


def read_hdfs_file_via_webhdfs(product_name):

    file_names = list_hdfs_directory_files(product_name)
    # For demonstration, read the first Parquet file found
    if file_names:
        parquet_file = next((name for name in file_names if name.endswith('.parquet')), None)
        if parquet_file:
            full_file_path = f"/hadoop/hdfs/youtube/{product_name}.parquet/{parquet_file}"
            df = read_parquet_file_via_webhdfs(full_file_path)
            return df
    return None

def main():
    # Dummy data generation and loading - replace with your actual data loading logic
    num_points = 105
    countries = ["Morocco", "France", "US", "Canada", "Nigeria"]

    webscrap = pd.DataFrame({
        'Produit': ['HP', 'Litfun', 'KitchenAid', 'Samsung TV', 'Bobby Jones'],
        'Catégorie': ['Électronique', 'Fashions', 'Cuisine', 'Électronique', 'Fashions'],
        'Sous-Catégorie': ['Ordinateurs', 'Chaussures', 'Ustensiles de cuisine', 'TV', 'Chemises'],
    })

    # Ajout de données supplémentaires
    additional_data = pd.DataFrame({
        'Produit': ['Dell', 'Dior', 'HENCKELS', 'Macbook Pro', 'Nike Air Max'],
        'Catégorie': ['Électronique', 'Fashions', 'Cuisine', 'Électronique', 'Fashions'],
        'Sous-Catégorie': ['Casques audio', 'Sacs à main', 'Couteaux de cuisine', 'Ordinateurs', 'Chaussures'],
    })

    # Concaténation de s DataFrames pour obtenir une base de données plus grande
    webscrap = pd.concat([webscrap, additional_data], ignore_index=True)

    data = {country: generate_random_data(num_points) for country in countries}
    df = pd.DataFrame(data)
    # Assume commentaire is a DataFrame with a 'text' column - replace with your actual data loading logic
    commentaire = pd.read_csv("streamlit_app/macbook.csv")

    search_query, selected_category, selected_subcategory, selected_product = setup_sidebar(webscrap)

    sentiment_df = read_hdfs_file_via_webhdfs(selected_product)
    st.write(sentiment_df)

    if 'trigger_status' not in st.session_state:
            st.session_state.trigger_status = ''

    if st.sidebar.button("Trigger Airflow DAG"):
        response = trigger_dag("youtube_data", st.session_state.selected_product)

        if response.status_code == 200:
            st.session_state.trigger_status = "DAG triggered successfully!"
        else:
            st.session_state.trigger_status = f"Failed to trigger DAG. Status Code: {response.status_code}"

    st.sidebar.write(st.session_state.trigger_status)

    filtered_data = filter_data(search_query, selected_category, selected_subcategory, selected_product, webscrap, df)

    display_content(selected_product, df, countries)
    st.subheader("WordCloud")
    generate_wordcloud(' '.join(commentaire['text']))

if __name__ == "__main__":
    main()
