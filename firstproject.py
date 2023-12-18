from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

# api function creation

def Api_connect():
  Api_id="AIzaSyCEcAyzHNhHicE5sgSA5eT4kQwoFzQpalA"
  api_service_name="youtube"
  api_version="v3"

  youtube=build(api_service_name,api_version,developerKey=Api_id)

  return youtube
  
  youtube=Api_connect()


#creating channels info
def get_channel_info(channel_id):
    youtube=Api_connect()
    request=youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id

    )
    response=request.execute()
    for i in response['items']:
      data=dict(Channel_Name=i['snippet']['title'],
                Channel_id=i['id'],
                Subscribers=i['statistics']['subscriberCount'],
                views=i['statistics']['viewCount'],
                total_views=i['statistics']['videoCount'],
                channel_description=i['snippet']['description'],
                playlist_id=i['contentDetails']['relatedPlaylists']['uploads'])
      return data
    
#create video ids
def get_video_ids(channel_id):
    video_ids=[]
    youtube=Api_connect()
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range (len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids
    

# FUNCTION TO GET VIDEO DETAILS
youtube=Api_connect()
def get_video_info(video_ids):
    video_data=[]
    
    for video_id in video_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=video_id
        )
        response=request.execute()
        for item in response['items']:
            data = dict(Channel_name = item['snippet']['channelTitle'],
                                Channel_id = item['snippet']['channelId'],
                                Video_id = item['id'],
                                Title = item['snippet']['title'],
                                Thumbnail = item['snippet']['thumbnails']['default']['url'],
                                Description = item['snippet'].get('description'),
                                Published_date = item['snippet']['publishedAt'],
                                Duration = item['contentDetails']['duration'],
                                Views = item['statistics'].get('viewCount'),
                                Likes=item['statistics'].get('likecount'),
                                Comments = item['statistics'].get('commentCount'),
                                Favorite_count = item['statistics']['favoriteCount'],
                                Definition = item['contentDetails']['definition'],
                                Caption_status = item['contentDetails']['caption']
                               )
            video_data.append(data)
    return video_data


#get comment details
def get_comment_info(video_ids):
        Comment_data=[]
        try:
            for video_id in video_ids:
                request=youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    maxResults=50
                )
                response=request.execute()

                for item in response['items']:
                    data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                            Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)

        except:
            pass      
        return Comment_data     
                

#get_playlist_details

def get_playlist_details(channel_id):
    next_page_token=None
    All_data=[]
    while True:
        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()

        for item in response['items']:
            data=dict(Playlist_Id=item['id'],
                    Title=item['snippet']['title'],
                    Channel_Id=item['snippet']['channelId'],
                    Channel_Name=item['snippet']['channelTitle'],
                    PublishedAt=item['snippet']['publishedAt'],
                    Video_Count=item['contentDetails']['itemCount'])
            All_data.append(data)
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data

#upload to mongodb
client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["youtube_data"]

#get channel_details
def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_details})
    return "upload completed successfully"

#create channel_table
def channels_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="1122",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_id varchar(80) primary key,
                                                            Subscribers bigint,
                                                            views bigint,
                                                            total_views int,
                                                            channel_description text,
                                                            playlist_id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("created")

    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)


    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_Name ,
                                            Channel_id,
                                            Subscribers,
                                            views,
                                            total_views,
                                            channel_description,
                                            playlist_id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_id'],
                row['Subscribers'],
                row['views'],
                row['total_views'],
                row['channel_description'],
                row['playlist_id'])
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("inserted")

#create playlist_table
def playlist_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="1122",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Title varchar(80),
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_Count int)'''
    cursor.execute(create_query)
    mydb.commit()


    pl_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=pd.DataFrame(pl_list)


    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="1122",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    for index,row in df1.iterrows():
            insert_query='''insert into playlists(Playlist_Id ,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                PublishedAt,
                                                Video_Count)
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''
            values=(row['Playlist_Id'],
                    row['Title'],
                    row['Channel_Id'],
                    row['Channel_Name'],
                    row['PublishedAt'],
                    row['Video_Count'])
        
            cursor.execute(insert_query,values)
            mydb.commit()
    
#create videos_table
def videos_table():
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="1122",
                            database="youtube_data",
                            port="5432")
    cursor = mydb.cursor()

    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table if not exists videos(
                        Channel_name varchar(100),
                        Channel_id varchar(100),
                        Video_id varchar(50),
                        Title varchar(150),
                        Thumbnail varchar(200),
                        Description text,
                        Published_date timestamp,
                        Duration interval,
                        Views bigint,
                        Likes bigint,
                        Comments int,
                        Favorite_count int,
                        Definition varchar(10),
                        Caption_status varchar(50))'''

    cursor.execute(create_query)
    mydb.commit()

    vi_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)

    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="1122",
                            database="youtube_data",
                            port="5432")
    cursor = mydb.cursor()
    for index, row in df2.iterrows():
        try:
            # Replace NaN values with appropriate values
            row = row.fillna({'Views': 0, 'Likes': 0, 'Comments': 0, 'Favorite_count': 0, 'Duration': pd.NaT})

            insert_query = '''insert into videos(Channel_name,
                                                Channel_id,
                                                Video_id,
                                                Title,
                                                Thumbnail,
                                                Description,
                                                Published_date,
                                                Duration,
                                                Views,
                                                Likes,
                                                Comments,
                                                Favorite_count,
                                                Definition,
                                                Caption_status)
                                                                    
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

            values = (
                row['Channel_name'],
                row['Channel_id'],
                row['Video_id'],
                row['Title'],
                row['Thumbnail'],
                row['Description'],
                row['Published_date'],
                row['Duration'],
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Favorite_count'],
                row['Definition'],
                row['Caption_status']
            )

            cursor.execute(insert_query, values)
            mydb.commit()
            print(f"Row {index} inserted successfully.")
        except Exception as e:
            print(f"Error inserting row {index}: {e}")
            print(f"Problematic row data: {row}")

    print("Data insertion complete.")

# Call the function
videos_table()


#create comments_table
def comments_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="1122",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp)'''
                            
                                                        
    cursor.execute(create_query)
    mydb.commit()

    com_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=pd.DataFrame(com_list)

    for index,row in df3.iterrows():
                insert_query='''insert into comments(Comment_Id,
                                                    Video_Id,
                                                    Comment_Text,
                                                    Comment_Author,
                                                    Comment_Published)
                                                    
                                                    values(%s,%s,%s,%s,%s)'''
                values=(row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published'])
            
                cursor.execute(insert_query,values)
                mydb.commit()
        
#create function for whole
def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return"tables created successfully"

#create show_channel table:
def show_channel_table():
    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df
#create show_playlists table:
def show_playlists_table():
    pl_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)

    return df1
#create show_videos table:
def show_videos_table():
    vi_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = st.dataframe(vi_list)

    return df2
#create show_comments table:
def show_comments_table():
    com_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)

    return df3

#streamlit part

with st.sidebar:
    st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("outcomes")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption(" Data Managment using MongoDB and SQL")

channel_id = st.text_input("Drop the Channel id")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Collect and Store data"):
    ch_ids = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_id"])
    if channel_id in ch_ids:
        st.success("Channel details of the given channel id already exists")
    else:
        insert = channel_details(channel_id)
        st.success(insert)
if st.button("Migrate to sql"):
    Table=tables()
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channel_table()
elif show_table=="PLAYLISTS":
    show_playlists_table()
elif show_table=="VIDEOS":
    show_videos_table()
elif show_table=="COMMENTS":
    show_comments_table()

#sql connection
mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="1122",
                    database="youtube_data",
                    port="5432")
cursor=mydb.cursor()

question=st.selectbox("choose your question",("1.What are the names of all the videos and their corresponding channels?",
                                              "2.Which channels have the most number of videos, and how many videos do they have?",
                                              "3.What are the top 10 most viewed videos and their respective channels?",
                                              "4.How many comments were made on each video, and what are their corresponding video names?",
                                              "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                              "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                              "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                              "8.What are the names of all the channels that have published videos in the year 2022?",
                                              "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                              "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))




if question == '1.What are the names of all the videos and their corresponding channels?':
    query1 = "select Title as videos, Channel_Name as ChannelName from videos;"
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))

elif question == '2.Which channels have the most number of videos, and how many videos do  they have?':
    query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

elif question == '3.What are the top 10 most viewed videos and their respective channels?':
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))

elif question == '4.How many comments were made on each video, and what are their corresponding video names?':
    query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

elif question == '5.Which videos have the highest number of likes, and what are their corresponding channel names?':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))

elif question == '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["like count","video title"]))

elif question == '7.What is the total number of views for each channel, and what are their corresponding channel names?':
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

elif question == '8.What are the names of all the channels that have published videos in the year 2022?':
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

elif question == '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))

elif question == '10.Which videos have the highest number of comments, and what are their corresponding channel names?':
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                       where Comments is not null order by Comments desc;'''
    cursor.execute(query10)




