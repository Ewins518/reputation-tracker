import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import matplotlib.pyplot as plt
from wordcloud import WordCloud

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


def return_selected_product():
    return

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

    filtered_data = filter_data(search_query, selected_category, selected_subcategory, selected_product, webscrap, df)

    display_content(selected_product, df, countries)
    st.subheader("WordCloud")
    generate_wordcloud(' '.join(commentaire['text']))

if __name__ == "__main__":
    main()