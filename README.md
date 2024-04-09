# youtube

****#Problem Statement ****
To create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.The application should contain the fetaures that includes Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, comments of each video) using Google API.Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.Option to store the data in a MYSQL or PostgreSQL.Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.

**#Project Overview**
The YouTube Data Harvesting and Warehousing project is dedicated to creating a Streamlit application that provides users with access to and analysis of data from multiple YouTube channels.

**#Project Features:**
Data Retrieval:
Users can input a YouTube channel ID and retrieve relevant data from the YouTube channels.
Data includes channel name, subscribers, total video count, likes, dislikes, comments, and more.
Store Data:
The app allows users to collect and store data for up to 10 different YouTube channels in a database with a simple button click.
Data Analysis:
Data stored in MySQL can be retrieved using different search options, enabling users to analyze and derive insights from the collected data.

**#Technologies and Frameworks Used:**
Python: The application is built using Python programming language, leveraging its rich ecosystem of libraries and tools for data retrieval, processing, and visualization.
Streamlit: The front-end interface and user interactions are developed using Streamlit, a popular open-source app framework for Machine Learning and Data Science.
Google API: The app integrates the YouTube Data API for fetching data from YouTube channels.
MySQL: Data storage and retrieval are facilitated using MySQL database.

## Before Getting Started-----Prerequisites

Before running the code, follow these steps:

1. Obtain the required API keys and credentials for MySQL .

2. Set up the API credentials and connection details for MySQL.

### Running the Application

1. Run the Streamlit app using the following command: streamlit run youtube.py (you can give your filename.py) 
