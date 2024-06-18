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
from streamlit import session_state as state
import time
import folium
from streamlit_folium import folium_static


# Dynamically add the project root to sys.path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
from webscraping.main import getCategoriesNames,get_sub_categorie,get_sub_sub_categories,get_products_from_page
from dags.trigger_dag import trigger_dag

st.set_option('deprecation.showPyplotGlobalUse', False)

coordinates = {
    "Morocco": [34.02, -6.83],
    "France": [48.86, 2.35],
    "Japan": [35.68, 139.76],
    "USA": [40.71, -74.01],
    "South Africa": [-26.2, 28.04]
}

def generate_wordcloud(text, font_path='streamlit_app/Amiri/Amiri-Regular.ttf'):
    wordcloud = WordCloud(width=800, height=400, max_words=200,  font_path=font_path,background_color='white').generate(text)
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    st.pyplot()


def clear_subcategory_state():
    """Clears subcategory selection when category changes."""
    if 'selected_subcategory' in st.session_state:
        del st.session_state['selected_subcategory']

import streamlit as st

def select_category():
    st.sidebar.title("Recherche")
    search_query = st.sidebar.text_input("Produit, catégorie ou sous-catégorie :", "")

    st.sidebar.title("Filtres")
    #categories = getCategoriesNames()
    categories = ['All Departments', 'Alexa Skills', 'Amazon Devices', 'Amazon Global Store', 'Amazon Warehouse', 'Apps & Games', 'Audible Audiobooks', 'Automotive', 'Baby', 'Books', 'Camera & Photo', 'CDs & Vinyl', 'Classical Music', 'Computers & Accessories', 'Deals', 'Digital Music', 'DVD & Blu-ray', 'Electronics & Photo', 'Fashion', '   Women', '   Men', '   Girls', '   Boys', '   Baby', 'Garden & Outdoors', 'Gift Cards', 'Grocery', 'Handmade', 'Health & Personal Care', 'Home & Business Services', 'Home & Kitchen', 'Home Improvement', 'Industrial & Scientific', 'Kindle Store', 'Large Appliances', 'Lighting', 'Luggage and travel gear', 'Luxury Stores', 'Magazines', 'Musical Instruments & DJ Equipment', 'Office Products', 'PC & Video Games', 'Perfume & Cosmetic', 'Pet Supplies', 'Premium Beauty', 'Prime Video', 'Software', 'Sports', 'Subscribe & Save', 'tegut...', 'Toys & Games']
    # Adding a placeholder at the beginning of the categories list
    options = ["Select a category"] + categories[1:]
    selected_category = st.sidebar.selectbox(
        "Catégorie",
        options=options,
        index=0,  # Default to the placeholder
        key='category',
        on_change=clear_subcategory_state
    )
    if selected_category != "Select a category":
        st.session_state['selected_category'] = selected_category
    else:
        selected_category = None
    
    return search_query, selected_category



def clear_product_state():
    """Clears product selection when sub-subcategory changes."""
    if 'selected_product' in st.session_state:
        del st.session_state['selected_product']

def select_subsubcategory_or_product(subcategory_dict):
    if 'selected_subcategory' in st.session_state:
        # Proceed only if a valid subcategory is selected and present in the session state
        subsubcategories = get_sub_sub_categories(subcategory_dict[st.session_state['selected_subcategory']])
        subsubcategory_dict = {name: url for name, url in subsubcategories} if subsubcategories else {}

        # Decide whether to show sub-subcategories or products based on availability
        if subsubcategories:
            selected_subsubcategory = st.sidebar.selectbox(
                "Sous-Sous-Catégorie",
                options = ["Select the SubSubcategory"] + list(subsubcategory_dict.keys()),
                index=0,
                key='subsubcategory',
                on_change=clear_product_state
            )
            if selected_subsubcategory != "Select the SubSubcategory":
                st.session_state['selected_subsubcategory'] = selected_subsubcategory

                # Load products based on the selected sub-subcategory
                products = get_products_from_page(subsubcategory_dict[selected_subsubcategory])
            else:
                selected_subsubcategory = None
                products = []
        else:
            # No sub-subcategories available, load products directly from the subcategory
            selected_subsubcategory = None
            products = get_products_from_page(subcategory_dict[st.session_state['selected_subcategory']])
        
        selected_product = st.sidebar.selectbox("Produit", options = ["Select the product"] + list(products), index=0, key='product')
        st.session_state['selected_product'] = selected_product
        return selected_subsubcategory, selected_product
    else:
        return None, None


def select_subcategory(selected_category):
    subcategories = get_sub_categorie(selected_category)
    subcategory_dict = {name: url for name, url in subcategories}
    selected_subcategory = st.sidebar.selectbox("Sous-Catégorie",
                                                    options = ["Select the Subcategory"] + list(subcategory_dict.keys()),
                                                    index=0, key='subcategory')
    if selected_subcategory != "Select the Subcategory":
        st.session_state['selected_subcategory'] = selected_subcategory
    else:
        selected_subcategory = None

    return subcategory_dict, selected_subcategory

def setup_sidebar():
    search_query, selected_category = select_category()
    st.session_state.trigger_status = ""
    selected_subcategory = None
    selected_product = None
    if selected_category and 'selected_category' in st.session_state:
        subcategory_dict, selected_subcategory = select_subcategory(st.session_state['selected_category'])

        if selected_subcategory and 'selected_subcategory' in st.session_state:
            selected_subsubcategory, selected_product = select_subsubcategory_or_product(subcategory_dict)

    return selected_product




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


def update_country():
    st.session_state.selected_country = st.session_state.country_select
    #st.rerun()



def display_content(selected_product, df):
    st.title(f"{selected_product} Reputation")

    row1 = st.columns(3)
    row2 = st.columns(2)

    # Calculate the number of positive and negative comments
    sentiment_counts = df['sentiment'].value_counts()
    total_comments = len(df)
    positive_comments = sentiment_counts.get(1, 0)
    negative_comments = sentiment_counts.get(0, 0)

    with row1[0]:
        container = st.container()
        container.markdown("**Total Comments**")
        container.write(total_comments)

    with row1[1]:
        container = st.container()
        container.markdown("**Positive Comments**")
        container.write(positive_comments)

    with row1[2]:
        container = st.container()
        container.markdown("**Negative Comments**")
        container.write(negative_comments)

    # Select a country
    country_list = df['country'].unique().tolist()

    with row2[0]:
        if 'selected_country' not in st.session_state:
            st.session_state['selected_country'] = country_list[0]

        selected_country = st.selectbox(
            "Select a country",
            country_list,
            key='country_select',
            index=country_list.index(st.session_state.selected_country),
            on_change=update_country
        )

        selected_country = st.session_state.selected_country

        # Filter the DataFrame by the selected country
        country_df = df[df['country'] == selected_country]

        if not country_df.empty:
            fig_donut = px.pie(country_df, names='sentiment', hole=0.4,
                               title='Sentiment Distribution',
                               color='sentiment', 
                               color_discrete_map={0: 'red', 1: 'green'})
            st.plotly_chart(fig_donut, use_container_width=True)
        else:
            st.write("No comments for the selected country.")


    # Plot the pie chart for sentiments
    with row2[1]:
        map_data = []
        for country in df['country'].unique():
            if country in coordinates:
                coord = coordinates[country]
                sentiment_df = df[df['country'] == country]
                pos_count = sentiment_df[sentiment_df['sentiment'] == 1].shape[0]
                neg_count = sentiment_df[sentiment_df['sentiment'] == 0].shape[0]
                color = 'green' if pos_count > neg_count else 'red'
                map_data.append({
                    'country': country,
                    'latitude': coord[0],
                    'longitude': coord[1],
                    'color': color,
                    'pos_count': pos_count,
                    'neg_count': neg_count
                })

        if map_data:
            m = folium.Map(location=[20, 0], zoom_start=1)
            for data in map_data:
                folium.Marker(
                    location=[data['latitude'], data['longitude']],
                    popup=f"{data['country']}: Positive - {data['pos_count']}, Negative - {data['neg_count']}",
                    icon=folium.Icon(color=data['color']) 
                ).add_to(m)
            st.subheader("Comments Location")
            folium_static(m,400,400)


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

def trigger_and_wait_for_file(selected_product):
    """Triggers an Airflow DAG and waits for the output file to appear in HDFS."""
    response = trigger_dag("youtube_data", selected_product)
    if response.status_code == 200:
        st.session_state.trigger_status = "DAG triggered successfully!"
        print("je suis la")
        time.sleep(180)
        # Set the expected HDFS directory based on the product name
        directory_path = f"/hadoop/hdfs/youtube/{selected_product}.parquet/"
        for _ in range(10):  # Limit the number of attempts to 10
            file_names = list_hdfs_directory_files(selected_product)
            parquet_file = next((name for name in file_names if name.endswith('.parquet')), None)
            if parquet_file:
                st.session_state.trigger_status = "File is ready in HDFS."
                return read_parquet_file_via_webhdfs(directory_path + parquet_file)
            time.sleep(30)  # Wait for 30 seconds before retrying
        st.session_state.trigger_status = "File not found in HDFS after waiting."
    else:
        st.session_state.trigger_status = f"Failed to trigger DAG. Status Code: {response.status_code}"
    return None


def main():
    selected_product = setup_sidebar() 
    #selected_product = "Fenzy"
    button = st.sidebar.button("Trigger Airflow DAG")
    if 'trigger_status' not in st.session_state:
        st.session_state['trigger_status'] = ""

    if 'random' not in st.session_state:
        st.session_state['random'] = ""
    
    #st.session_state.trigger_status = ""

    centering_css = """
        <style>
            .centered {
                position: absolute;
                color: white;
                text-align: center;
            }
        </style>"""
    st.markdown(centering_css, unsafe_allow_html=True)

    welcome_message_placeholder = st.empty()

    if st.session_state['trigger_status'] == "":
        # Display the welcome message if no product is selected
        welcome_message_placeholder.markdown("<div class='centered'><h1>Welcome to Amazon Products Reputation Dashboard</h1></div>", unsafe_allow_html=True)
    
    if button or st.session_state.trigger_status != "":
        st.session_state.trigger_status = "DAG triggered successfully!"
        welcome_message_placeholder.empty()

        with st.spinner('Please wait while the DAG is being triggered...'):
            sentiment_df = read_hdfs_file_via_webhdfs(selected_product)

            if sentiment_df is None:
                sentiment_df = trigger_and_wait_for_file(selected_product)

            st.session_state.trigger_status = "DAG triggered successfully!"
           
            display_content(selected_product, sentiment_df)
            st.subheader("WordCloud")
            generate_wordcloud(' '.join(sentiment_df['text']))

if __name__ == "__main__":
    main()
