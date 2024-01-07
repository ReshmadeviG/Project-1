# **YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit**


# Overview
This project is focused on harvesting data from YouTube channels using the YouTube API, processing the data, and warehousing it. The harvested data is initially stored in a MongoDB Atlas database as documents and is then converted into SQL records for in-depth data analysis. The project's core functionality relies on the Extract, Transform, Load (ETL) process.

# Approach
* Harvest YouTube channel data using the YouTube API by providing a 'Channel ID'.
* Store channel data in MongoDB Atlas as documents.
* Convert MongoDB data into SQL records for data analysis.
* Implement Streamlit to present code and data in a user-friendly UI.
* Execute data analysis using SQL queries and Python integration.

# Getting Started
* Install/Import the necessary modules: Streamlit, PyMongo, MySQL and Googleapiclient
* Ensure you have access to MongoDB Atlas

# Technical Steps to Execute the Project
* Step 1: Install/Import Modules
Ensure the required Python modules are installed: Streamlit, PyMongo, MySQL and Googleapiclient.
* Step 2: Run the Project with Streamlit
Open the command prompt in the directory where "projectyoutube.py" is located.
Execute the command: streamlit run projectyoutube.py. This will open a web browser, such as Google Chrome, displaying the project's user interface.
* Step 3: Configure Databases
Ensure that you are connected to both MongoDB Atlas and your MYSQL Localhost.

# Methods
* Get YouTube Channel Data: Fetches YouTube channel data using a Channel ID and creates channel details in JSON format.
* Get Playlist Videos: Retrieves all video IDs for a provided playlist ID.
* Get Video and Comment Details: Returns video and comment details for the given video IDs.
* Get All Channel Details: Provides channel, video, and playlist details in JSON format.
* Merge Channel Data: Combines channel details, video details, and playlist details into a single JSON format.
* Insert Data into MongoDB: Inserts channel data into MongoDB Atlas as a document.
* Get Channel Names from MongoDB: Retrieves channel names from MongoDB documents.
* Convert MongoDB Document to Dataframe: Fetches MongoDB documents and converts them into dataframes for SQL data insertion.
* Data Transformation for SQL: Performs data transformation for loading into SQL.
* Data Load to SQL: Loads data into SQL.

# Data Analysis: 
*Conducts data analysis using SQL queries and Python integration.

# Tools Expertise
* Python (Scripting)
* Data Collection
* MongoDB
* SQL
* API Integration
* Data Management using MongoDB (Atlas) and MYSQL
* Streamlit

# Result :
This project focuses on harvesting YouTube data using the YouTube API, storing it in MongoDB, converting to SQL for analysis. Utilizes Streamlit, Python, and various methods for ETL. Expertise includes Python, MongoDB, SQL, API integration, and data management tools . This project maily reduces 80% percentage of manually data processing and data storing work effectively.
