# Big Data Architecture for Products Reputation and Tracking

## Demo Video

https://github.com/Ewins518/project-S3/assets/74628423/b014a1ed-c924-4f07-a551-64af52234d42


## Project Overview

## Project Overview
The "Big Data Architecture for Products Reputation and Tracking" project aims to build a scalable, robust, and efficient system for monitoring and analyzing amazon product reputation. The architecture leverages various big data technologies to process and visualize data from youtube API, providing valuable insights into product sentiment and customer feedback.

![Screenshot](/photos/projects3_arch.png)

## Architecture Components

### Data Sources:
- **Amazon**: Used to get the product name,its categories, subcategories,... by a dynamically webscraping
- **YouTube API**: Used to fetch product-related comments and reviews from YouTube

### Data Ingestion:
- **Apache Kafka**: Serves as the data ingestion layer, capturing real-time data streams from YouTube and Amazon. Kafka ensures that data is ingested efficiently and is available for downstream processing.

### Data Processing:
- **Apache Spark**: Processes the ingested data from Kafka. Spark performs transformations and analytics, including sentiment analysis using a pre-trained LLM model. This allows the system to categorize reviews as positive or negative.

### Data Storage:
- **Hadoop HDFS**: The processed data is stored in HDFS, providing a scalable and fault-tolerant storage solution. HDFS ensures that large volumes of data can be stored and accessed efficiently.

### Orchestration and Workflow Management:
- **Apache Airflow**: Manages and orchestrates the data pipelines. Airflow triggers and monitors the data processing workflows, ensuring that data is processed and updated regularly.

### Data Visualization:
- **Streamlit**: The processed data is visualized using Streamlit. The dashboard provides interactive visualizations, including sentiment distribution, geographical distribution of comments, and word clouds of frequently mentioned terms.

### Microservices and Containerization:
**Docker**: The entire architecture is containerized using Docker. This ensures consistency across different environments and simplifies deployment and scaling of the system.


## Features

- **Real-Time Data Ingestion**: Captures real-time data from YouTube using Apache Kafka.

- **Scalable Data Processing**:Processes large volumes of data using Apache Spark, ensuring scalability and efficiency.

- **Robust Data Storage**: Utilizes Hadoop HDFS for scalable and fault-tolerant data storage.

- **Automated Workflows**: Orchestrates data pipelines using Apache Airflow, ensuring automated and timely data processing.

- **Interactive Dashboards**: Visualizes data using Streamlit, providing interactive and insightful visualizations.

- **Sentiment Analysis**: Analyzes product-related comments from youtube to determine the sentiment using a pre-trained LLM model.

- **Geographical Insights**: Displays the geographical distribution of product reviews.




