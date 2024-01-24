# YouTube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit

## INTRODUCTION
This project is a YouTube API scrapper that allows users to retrieve and analyze data from YouTube channels. It utilizes the YouTube Data API to fetch information such as channel statistics, video details, comments, and more. The scrapper provides various functionalities to extract and process YouTube data for further analysis and insights.
## PROBLEM STATEMENT:
YouTube, being a vast source of video content, presents an opportunity to gather valuable insights into channel and video performance. The objective of this project is to develop a system that efficiently harvests data from YouTube channels, stores it in a data warehouse for structured analysis, and provides a user-friendly interface for querying and visualizing the data.

## TECHNOLOGIES USED:
**1.	Python:**
The entire project is implemented using the Python programming language, showcasing its versatility and extensive ecosystem for data handling and web development.

**2.	YouTube Data API:**
	The official YouTube Data API is employed to interact with YouTube's platform, facilitating the retrieval of channel, video, and comment data. This API is crucial for accessing and collecting information from YouTube.
 
 **3.	PyMongo:**
	PyMongo is a Python library that acts as a connector between Python and MongoDB. It facilitates the interaction with MongoDB from the YouTube Data Scraper, allowing for seamless data storage and retrieval.
 
**4.	MongoDB:**
	MongoDB serves as the NoSQL database for efficient storage of raw, unstructured data. It provides flexibility in handling diverse data types and is well-suited for the initial storage of YouTube data.
 
**5.	PostgreSQL:**
	PostgreSQL, a robust open-source relational database management system, is used for structured data storage and management. It supports efficient querying and analysis, making it suitable for structured tables related to channels, playlists, videos, and comments.

**6.	Pandas:**
	Pandas, a powerful data manipulation and analysis library in Python, is employed in the YouTube Data Scraper. It provides essential functionalities for handling and processing data retrieved from YouTube, such as filtering, transformation, and aggregation.

 **7.	Streamlit:**
	Streamlit is utilized to build the user interface and visualization components of the project. It simplifies the process of creating interactive web applications with minimal code, enhancing the user experience.
 


## KEY COMPONENTS:
•	_API Integration:_   Utilizes the YouTube Data API to fetch channel information, video details, and comments.

•	_Data Storage:_   MongoDB is used for storing the raw data in a structured format.

•	_SQL Database:_  Data is migrated to a PostgreSQL database for structured querying and analysis.

•	_Tables Creation:_  Separate tables are created for channels, playlists, videos, and comments in the SQL database.

•	_Streamlit App:_  A Streamlit app is developed for interactive data exploration and querying.

•	_Data Exploration Queries:_ SQL queries are implemented to answer specific questions about the data.

•	_Streamlit App Features:_  User input for providing YouTube channel IDs.Buttons for data collection, migration to SQL, and displaying tables.Radio button for selecting the table to view.

__SQL Queries:__
•	Videos and their corresponding channels.

•	Channels with the most number of videos.

•	Top 10 most viewed videos and their channels.

•	Number of comments on each video.

•	Videos with the highest number of likes and their channels.

•	Total likes and dislikes for each video.

•	Total views for each channel.

•	Channels that published videos in 2022.

•	Average duration of videos in each channel.

•	Videos with the highest number of comments and their channels.

## STEPS:

1. Establish connection to YouTube API using Google API Client and developer key.
2. Create functions to extract channel info, video IDs, details, and comments from YouTube API.
3. Utilize Pymongo for MongoDB and Psycopg2 for PostgreSQL to store data.
4. Run SQL queries on PostgreSQL for data analysis, answering specific YouTube data questions.
5. Leverage Pandas for efficient data manipulation tasks.
6. Integrate Streamlit for user-friendly web app allowing data extraction and visualization.
7. Use Pandas to create visualizations like bar charts for summarized insights.
8. Showcase a holistic project approach providing valuable insights into YouTube channels, videos, and user engagement.

## CONCLUSION:
The project integrates YouTube API, MongoDB, SQL, and Streamlit for a comprehensive data harvesting and warehousing solution.Enables easy data exploration and analysis through the Streamlit app.Facilitates querying insights from the SQL database.

