#importing required libraries
import os
from googleapiclient.discovery import build
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import mysql.connector as db
import streamlit as st

#API key connection
def connect_api():
    api_id="your api key"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=api_id)
    return youtube
youtube=connect_api()

#sql connection
mydb = db.connect(
    host='your host name',
    user='your user id',
    password='your password',
    database='your mysql database')
cursor = mydb.cursor()

#Fetch channel details
def get_channel_info(channel_id):
    youtube=connect_api()
    channel_data=[]
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    for item in response.get("items",[]):
        data = dict(
            channel_id= item["id"],
            channel_name= item["snippet"]["title"],
            channel_views= item["statistics"]["viewCount"],
            channel_description= item["snippet"]["description"],
            subscribers= item["statistics"]["subscriberCount"],
            total_videos= item["statistics"]["videoCount"],
            playlist_id= item["contentDetails"]["relatedPlaylists"]["uploads"]
        ) 
        channel_data.append(data)  
    return channel_data


# Fetch playlist details
def get_playlist_details(channel_id):
    youtube=connect_api()
    all_playlists=[]
    next_page_token = None
    while True:
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        for item in response.get("items",[]):
            data = dict(
                playlist_id= item["id"],
                playlist_name= item["snippet"]["title"],
                channel_id= item["snippet"]["channelId"])
            all_playlists.append(data)

            next_page_token = response.get("nextPageToken")
        if next_page_token is None:
            break
    return all_playlists

#Fetch video IDs
def get_video_ids(channel_id):
    youtube=connect_api()
    all_video_ids = []
    response = youtube.channels().list(id=channel_id, part="contentDetails").execute()
    playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    next_page_token = None
    while True:
        response = youtube.playlistItems().list(part="snippet",
                                                playlistId=playlist_id, 
                                                maxResults=50, 
                                                pageToken=next_page_token).execute()
        for item in response["items"]:
            video_id = item["snippet"]["resourceId"]["videoId"]
            all_video_ids.append(video_id)
           
        next_page_token = response.get("nextPageToken")
        if next_page_token is None:
            break
    return all_video_ids

#Fetch video information
def get_video_info(video_id):
    youtube=connect_api()
    all_video_info=[]
    request = youtube.videos().list(part="snippet,contentDetails,statistics", id=video_id)
    try:
        response = request.execute()
        for item in response.get("items",[]):
            data = dict(
                channel_name= item["snippet"]["channelTitle"],
                channel_id= item["snippet"]["channelId"],
                video_id= item["id"],
                video_name=item["snippet"]["title"],
                video_description= item["snippet"]["description"],
                published_date= item["snippet"]["publishedAt"].replace('T',' ').replace('Z',''),
                view_count= item["statistics"]["viewCount"],
                like_count=item["statistics"]["likeCount"],
                favorite_count=item["statistics"]["favoriteCount"],
                comment_count=item["statistics"]["commentCount"],
                duration= item["contentDetails"]["duration"],
                thumbnail= item["snippet"]["thumbnails"]["default"]["url"] )
            all_video_info.append(data)
    except:
        pass
    return all_video_info 

#Fetch comment information
def get_comment_info(video_id):
    youtube=connect_api()
    all_comments=[]
    request = youtube.commentThreads().list(part="snippet,id", 
                                      videoId=video_id)
    try:
        response = request.execute()
        for item in response.get("items",[]):
            data = dict(
                comment_id=item["snippet"]["topLevelComment"]["id"],
                video_id= item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                comment_text= item["snippet"]["topLevelComment"]["snippet"].get("textDisplay"),
                comment_author= item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                comment_published_date= item["snippet"]["topLevelComment"]["snippet"]["publishedAt"].replace('T',' ').replace('Z',''))
            all_comments.append(data)
    except:
        pass
    return all_comments

#Channel ids that need to fetched
channel_ids=["UCtb4a_MW9WgADe3yC2KXxow",
             "UC3EIT1VMZvCCBe_NZgXNv4A",
             "UCIJag9x5ZN149YE6wvIN32Q",
             "UC0E4_WBCxz6_OG8smQwbOEA",
             "UCjbBFvK04b-O3foSNabdrOA"]


#DataFrame Creation-----channel_data,playlist_data,videoid_df,video_data,comment_data
#channel df------channel_data
def channel_data():
    try:
        youtube = connect_api()           
        all_channel_data = []  
        for channel_id in channel_ids:
            channels_data = get_channel_info(channel_id)
            all_channel_data.extend(channels_data)
    except:
        pass
    return all_channel_data
channel_df=channel_data()
channel_data = pd.DataFrame(channel_df)

#playlist df----playlist_data
def playlist_data():
    try:
        youtube=connect_api()
        all_channel_playlists = []
        for channel_id in channel_ids:
            playlists_data = get_playlist_details(channel_id)
            all_channel_playlists.extend(playlists_data)
    except:
        pass
    return all_channel_playlists
playlist_df=playlist_data()
playlist_data= pd.DataFrame(playlist_df)

#video id df-----videoid_df
def videoid_df():
    youtube=connect_api()
    all_video_ids_data = []
    all_video_ids=set()
    for channel_id in channel_ids:
        video_ids = get_video_ids(channel_id)
        unique_video_ids = list(set(video_ids) - all_video_ids)
        all_video_ids.update(video_ids) 
        channel_video_data = {
            "channel_id": channel_id,
            "video_ids": unique_video_ids
        }
        all_video_ids_data.append(channel_video_data)
    return all_video_ids_data
video_id_data=videoid_df()
to_explode = pd.DataFrame(video_id_data)
videoid_df=to_explode.explode("video_ids")

#video df------video_data
def video_data():
        try:
                youtube=connect_api()
                video_ids_all_channels = videoid_df["video_ids"]
                all_video_data = []
                for video_id in video_ids_all_channels:
                        video_info = get_video_info(video_id)
                        all_video_data.extend(video_info) 
        except:
                pass
        return all_video_data
video_data_df=video_data()
video_data = pd.DataFrame(video_data_df)

#comment df----comment_df
def comment_data():
    try:
        all_comments_data = []
        video_ids_per_channel = videoid_df["video_ids"]
        for  video_id in video_ids_per_channel:
            comment_info = get_comment_info(video_id)
            all_comments_data.extend(comment_info)
    except:
        pass
    return all_comments_data
comments_df=comment_data()
comment_data = pd.DataFrame(comments_df)


#MySql tables ---channel,playlist,video,comment
#channel table
def channel_tb():

    drop="drop table if exists channel"
    cursor.execute(drop)
    mydb.commit()
    cursor.execute("CREATE TABLE IF NOT EXISTS channel (channel_id VARCHAR(255)PRIMARY KEY, channel_name VARCHAR(255), channel_views INT, channel_description TEXT,  subscribers INT, total_videos INT, playlist_id VARCHAR(255))")

    for index, row in channel_data.iterrows():
        sql = "INSERT INTO channel (channel_id, channel_name,  channel_views, channel_description, subscribers, total_videos, playlist_id) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (row['channel_id'], row['channel_name'], row['channel_views'], row['channel_description'], row['subscribers'], row['total_videos'], row['playlist_id'])
        
    #commit the changes to mysql table
        try:
            cursor.execute(sql, val)
            mydb.commit()
        except:
            print("Channel Data are already inserted into MySQL DB")

# playlist table
def playlist_tb():
    
    drop="drop table if exists playlist"
    cursor.execute(drop)
    mydb.commit()
    cursor.execute("CREATE TABLE IF NOT EXISTS playlist (playlist_id VARCHAR(255)PRIMARY KEY, playlist_name VARCHAR(255), channel_id VARCHAR(255))")

    for index, row in playlist_data.iterrows():
        sql = "INSERT INTO playlist (playlist_id, playlist_name, channel_id) VALUES (%s, %s, %s)"
        val = (row["playlist_id"], row["playlist_name"], row["channel_id"])
        
        try:
            cursor.execute(sql, val)
            mydb.commit()
        except:
            print("Playlist Data are already inserted into MySQL DB")

#video table
def video_tb():
    
    cursor = mydb.cursor()
    drop="drop table if exists video"
    cursor.execute(drop)
    mydb.commit()
    cursor.execute("CREATE TABLE IF NOT EXISTS video (\
                channel_name VARCHAR(255),\
                    channel_id VARCHAR(255),\
                    video_id VARCHAR(255)PRIMARY KEY,\
                    video_name VARCHAR(255),\
                    video_description TEXT,\
                    published_date DATETIME,\
                    view_count INT,\
                    like_count INT,\
                    favorite_count INT,\
                    comment_count INT,\
                    duration VARCHAR(25),\
                    thumbnail VARCHAR(255))")

    for index, row in video_data.iterrows():
        sql = "INSERT INTO video (channel_name, channel_id, video_id, video_name, video_description, published_date, view_count, like_count, favorite_count, comment_count, duration, thumbnail) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (row["channel_name"], row["channel_id"], row["video_id"], row["video_name"], row["video_description"], row["published_date"], row["view_count"], row["like_count"], row["favorite_count"], row["comment_count"], row["duration"], row["thumbnail"])
        try:
            cursor.execute(sql, val)
            mydb.commit()
        except:
            print("Video Data are already inserted into MySQL DB")

#comment table
def comment_tb():
    
    drop="drop table if exists comment"
    cursor.execute(drop)
    mydb.commit()
    cursor.execute("CREATE TABLE IF NOT EXISTS comment (\
        comment_id VARCHAR(255)PRIMARY KEY,\
        video_id VARCHAR(255),\
        comment_text TEXT,\
        comment_author VARCHAR(255),\
        comment_published_date DATETIME\
    )")

    for index, row in comment_data.iterrows():
        sql = "INSERT INTO comment (comment_id, video_id, comment_text, comment_author, comment_published_date) VALUES (%s, %s, %s, %s, %s)"
        val = (row["comment_id"], row["video_id"], row["comment_text"], row["comment_author"], row["comment_published_date"])
        try:
            cursor.execute(sql, val)
            mydb.commit()
        except:
            print("Comment Data are already inserted into MySQL DB")

def mysql_tables():
    channel_tb()
    playlist_tb()
    video_tb()
    comment_tb()

    return ("Tables are created and values are stored ")

mysql_tbs=mysql_tables()

#visualizing data 
def channel_subs_view():
    plt.figure(figsize=(13,5))
    sns.set_theme(style="darkgrid")
    sns.barplot(data=channel_data,x="channel_name",y="subscribers")
    plt.gca().invert_yaxis()
    plt.title("Channels vs Subscribers")
    plt.show()
    return plt.show()

def channel_totvideos():
    plt.figure(figsize=(13,5))
    sns.set_theme(style="darkgrid")
    view=sns.barplot(data=channel_data,x="channel_name",y="total_videos")
    plt.gca().invert_yaxis()
    plt.title("Channels vs Total Videos")
    plt.show()
    return plt.show()

#Function for fetching particular channel's all information
def channel_info(channel_id):
    youtube=connect_api()
    channels_data = get_channel_info(channel_id)
    playlists_data = get_playlist_details(channel_id)
    video_ids = get_video_ids(channel_id)
    for video_id in video_ids:
        video_info = get_video_info(video_id)
        comment_info = get_comment_info(video_id)
    return channels_data,playlists_data,video_ids,video_info,comment_info


# Set Streamlit app 

st.set_page_config(layout="wide")

with st.sidebar:
    st.title(":blue[YouTube Data Harvesting and Warehousing]")
    st.header("Skills gained")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("Streamlit")
    st.caption("API integration")
    st.caption("MySQL Database")

channel_id=st.text_input("Enter the Channel Id")

channel_ids=["UCtb4a_MW9WgADe3yC2KXxow",
             "UC3EIT1VMZvCCBe_NZgXNv4A",
             "UCIJag9x5ZN149YE6wvIN32Q",
             "UC0E4_WBCxz6_OG8smQwbOEA",
             "UCjbBFvK04b-O3foSNabdrOA"]
channels_data = get_channel_info(channel_id)
playlists_data = get_playlist_details(channel_id)
video_ids = get_video_ids(channel_id)
for video_id in video_ids:
    video_info = get_video_info(video_id)
    comment_info = get_comment_info(video_id)

if st.button("Fetch Data"):
    
    if channel_id in channel_ids:
        st.success("Channel Data is already fetched and stored")
    else:
        channel_information = channel_info(channel_id)
        st.write(channel_information)

if st.button("Fetch all channels data"):
    view_dataframe=st.radio("SELECT THE DATA FOR VIEW",[ "Channels","Playlists","Videos","Comments" ])
    if view_dataframe=="Channels":
        channel_df=channel_data().append(channels_data)
        channel_data = pd.DataFrame(channel_df)
        channel_data
    if view_dataframe=="Playlists":
        playlist_df=playlist_data().append(playlists_data)
        playlist_data= pd.DataFrame(playlist_df)
        playlist_data
    if view_dataframe=="Videos":
        video_data_df=video_data().append(video_info)
        video_data = pd.DataFrame(video_data_df)
        video_data
    if view_dataframe=="Comments":
        comments_df=comment_data().append(comment_info)
        comment_data = pd.DataFrame(comments_df)
        comment_data

        
if st.button("Store data to MySQL DB"):
    table=mysql_tables()
    st.success(table)
    st.write("Channel data is stored successfully")

view_table=st.radio ("SELECT THE TABLE FOR VIEW",[ "Channels","Playlists","Videos","Comments"])
        

if view_table=="Channels":
    channel_tb()
if view_table=="Playlists":
    playlist_tb()
if view_table=="Videos":
    video_tb()
if view_table=="Comments":
    comment_tb()

with st.button("Channel Subscribers"):
    channel_subs_view()
with st.button("Channel Total Videos"):
    channel_totvideos()

#MySQL connection
mydb = db.connect(
        host='localhost',
        user='root',
        password='root',
        database='youtube')
cursor = mydb.cursor()
drop="drop table if exists channel"
cursor.execute(drop)
mydb.commit()

# SQL Query options
query_option = st.selectbox('Select SQL Query Option',
                            ['1.Video and Channel Names', '2.Channels with Most Videos',
                             '3.Top 10 Most Viewed Videos', '4.Comments Count by Video',
                             '5.Videos with Highest Likes', '6.Likes and Dislikes by Video Name',
                             '7.Total Views by Channel', '8.Channels with Videos in 2022',
                             '9.Average Video Duration by Channel', '10.Videos with Most Comments'])

# Execute SQL Query based on user selection
if query_option == '1.Video and Channel Names':
    query1 = "SELECT video_name, channel_name FROM video"
    cursor.execute(query1)
    mydb.commit()
    c1=cursor.fetchall()
    df1=pd.DataFrame(c1,columns=["Video Title","Channel Name"])
    st.write(df1)
elif query_option == '2.Channels with Most Videos':
    query2 = "SELECT channel_name, total_videos AS video_count FROM channel  ORDER BY video_count DESC"
    cursor.execute(query2)
    mydb.commit()
    c2=cursor.fetchall()
    df2=pd.DataFrame(c2,columns=["Channel Name","No.of Videos"])
    st.write(df2)
elif query_option == '3.Top 10 Most Viewed Videos':
    query3 = "SELECT view_count, channel_name, video_name  FROM video where view_count is not null ORDER BY view_count DESC LIMIT 10"
    cursor.execute(query3)
    mydb.commit()
    c3=cursor.fetchall()
    df3=pd.DataFrame(c3,columns=["Views","Channel Name","Video Title"])
    st.write(df3)
elif query_option == '4.Comments Count by Video':
    query4 = "SELECT comment_count, video_name FROM video where comment_count is not null"
    cursor.execute(query4)
    mydb.commit()
    c4=cursor.fetchall()
    df4=pd.DataFrame(c4,columns=["No.of Comments","Video Title"])
    st.write(df4)
elif query_option == '5.Videos with Highest Likes':
    query5 = "SELECT video_name, channel_name, like_count FROM video where like_count is not null ORDER BY like_count DESC LIMIT 1"
    cursor.execute(query5)
    mydb.commit()
    c5=cursor.fetchall()
    df5=pd.DataFrame(c5,columns=["Video Title","Column Name","Like Count"])
    st.write(df5)
elif query_option == '6.Likes by Video Name':
    query6 = "SELECT video_name, like_count FROM video "
    cursor.execute(query6)
    mydb.commit()
    c6=cursor.fetchall()
    df6=pd.DataFrame(c6,columns=["Video Title","Like Count"])
    st.write(df6)
elif query_option == '7.Total Views by Channel':
    query7 = "SELECT channel_name, channel_views AS total_views FROM channel"
    cursor.execute(query7)
    mydb.commit()
    c7=cursor.fetchall()
    df7=pd.DataFrame(c7,columns=["Channel name","Total views"])
    st.write(df7)
elif query_option == '8.Channels with Videos in 2022':
    query8 = "SELECT video_name,published_date, channel_name FROM video WHERE EXTRACT (YEAR FROM published_date) = 2022"
    cursor.execute(query8)
    mydb.commit()
    c8=cursor.fetchall()
    df8=pd.DataFrame(c8,columns=["Video Title","Published Date","Channel Name"])
    st.write(df8)
elif query_option == '9.Average Video Duration by Channel':
    query9 = "SELECT channel_name, AVG(duration) AS average_duration FROM video GROUP BY channel_name"
    cursor.execute(query9)
    mydb.commit()
    c9=cursor.fetchall()
    df9=pd.DataFrame(c9,columns=["Channel name","Average Duration"])
    C9=[]
    for index,row in df9.iterrows():
        channel_title=row["Channel name"]
        average_duration=row["Average Duration"]
        avg_duration_str=str(average_duration)
        C9.append(dict(channeltitle=channel_title,avgduration=avg_duration_str))
    df=pd.DataFrame(C9)
    st.write(df)

elif query_option == '10.Videos with Most Comments':
    query10 = "SELECT video_name, channel_name, comment_count FROM video where comment_count is not null ORDER BY comment_count DESC "
    cursor.execute(query10)
    mydb.commit()
    c10=cursor.fetchall()
    df10=pd.DataFrame(c10,columns=["Video Title","Channel Name,Comments"])
    st.write(df10)

# Close the MySQL connection
mydb.close()



    



    




