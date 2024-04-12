#import required libraries
from googleapiclient.discovery import build
import pandas as pd
import datetime
import plotly.graph_objects as go
import plotly.express as px
import isodate
import mysql.connector as db
import streamlit as st

#API Key connection
def connect_api():
    api_id="enter your api key"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=api_id)
    return youtube
youtube=connect_api()

#Fetch channel details

def get_channel_info(channel_id):
    try:
        channel_data=[]
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id)
        response = request.execute()
        for item in response.get("items",[]):
            data = {
                "channel_name": item["snippet"]["title"],
                "channel_id": item["id"],
                "channel_views": item["statistics"]["viewCount"],
                "channel_description": item["snippet"]["description"],
                "subscribers": item["statistics"]["subscriberCount"],
                "total_videos": item["statistics"]["videoCount"],
                "playlist_id": item["contentDetails"]["relatedPlaylists"]["uploads"]
            }
            channel_data.append(data)
        return channel_data                
    except:
        pass
    

# Fetch playlist details
def get_playlist_details(channel_id):
    try:
        playlists=[]
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
                playlists.append(data)

            next_page_token = response.get("nextPageToken")
            if next_page_token is None:
                break
    except :
        pass
    return playlists


 #Fetch video IDs
def get_video_ids(channel_id):
    try:
        youtube=connect_api()
        video_ids= []
        response = youtube.channels().list(id=channel_id, part="contentDetails").execute()
        playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        next_page_token = None
        while True:
            response1 = youtube.playlistItems().list(part="snippet",
                                                    playlistId=playlist_id, 
                                                    maxResults=50, 
                                                    pageToken=next_page_token).execute()
            for item in range(len(response1['items'])):
                video_ids.append(response1['items'][item]['snippet']['resourceId']['videoId'])
            next_page_token = response.get("nextPageToken")
            if next_page_token is None:
                break
    except:
        pass
    return video_ids
        

#Fetch Video Information
def get_video_info(channel_id):

    video=[]
    try:
        video_ids=get_video_ids(channel_id)
        for video_id in video_ids:
            request = youtube.videos().list(part="snippet,contentDetails,statistics", id=video_id)
        
            response = request.execute()
            for item in response.get("items",[]):
                duration = isodate.parse_duration(item["contentDetails"]["duration"])
                time_format = str(duration)
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
                    duration= time_format,
                    thumbnail= item["snippet"]["thumbnails"]["default"]["url"] )
                video.append(data)
    except:
        pass
    return video


#Fetch comment information
def get_comment_info(channel_id):
    
    comment=[]
    try:
        video_ids=get_video_ids(channel_id)
        for video_id in video_ids:
            request = youtube.commentThreads().list(part="snippet,id", 
                                            videoId=video_id)
        
            response = request.execute()
            for item in response.get("items",[]):
                data = dict(
                    comment_id=item["snippet"]["topLevelComment"]["id"],
                    video_id= item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                    comment_text= item["snippet"]["topLevelComment"]["snippet"].get("textDisplay"),
                    comment_author= item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    comment_published_date= item["snippet"]["topLevelComment"]["snippet"]["publishedAt"].replace('T',' ').replace('Z',''))
                comment.append(data)
    except:
        pass
    return comment


#MySql tables -----channel,playlist,video,comment
#channel table
def channel_tb(channel_id):
    
    mydb = db.connect(
        host='localhost',
        user='enter your username',
        password='enter your password',
        database='youtube')
    cursor = mydb.cursor()
    
    cursor.execute("CREATE TABLE IF NOT EXISTS channels ( channel_name VARCHAR(255), channel_id VARCHAR(255)PRIMARY KEY,channel_views INT, channel_description TEXT,  subscribers INT, total_videos INT, playlist_id VARCHAR(255))")
    channel_info=get_channel_info(channel_id)  
    channel_data=pd.DataFrame(channel_info)
    try:
        for index, row in channel_data.iterrows():
            sql = "INSERT INTO channels ( channel_name, channel_id,  channel_views, channel_description, subscribers, total_videos, playlist_id) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            val = ( row['channel_name'], row["channel_id"], row['channel_views'], row['channel_description'], row['subscribers'], row['total_videos'], row['playlist_id'])
            cursor.execute(sql, val)
            mydb.commit()
    except:
        print("Channel Data are already inserted into MySQL DB")

# playlist table
def playlist_tb(channel_id):

    mydb = db.connect(
        host='localhost',
        user='enter your username',
        password='enter your password',
        database='youtube')
    cursor = mydb.cursor()
    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS playlist (playlist_id VARCHAR(255)PRIMARY KEY, playlist_name VARCHAR(255), channel_id VARCHAR(255))")
        playlist_info=get_playlist_details(channel_id)
        playlist_data=pd.DataFrame(playlist_info)
        for index, row in playlist_data.iterrows():
            sql = "INSERT INTO playlist (playlist_id, playlist_name, channel_id) VALUES (%s, %s, %s)"
            val = (row["playlist_id"], row["playlist_name"], row["channel_id"])
            cursor.execute(sql, val)
            mydb.commit()
    except:
        print("Playlist Data are already inserted into MySQL DB")


#video table
def video_tb(channel_id):
    mydb = db.connect(
        host='localhost',
        user='enter your username',
        password='enter your password'
        database='youtube')
    cursor = mydb.cursor()
    
    try:
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
                        duration varchar(25),\
                        thumbnail VARCHAR(255))")
        video_info=get_video_info(channel_id)
        video_data=pd.DataFrame(video_info)
    
        for index, row in video_data.iterrows():
            sql = "INSERT INTO video (channel_name, channel_id, video_id, video_name, video_description, published_date, view_count, like_count, favorite_count, comment_count, duration, thumbnail) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (row["channel_name"], row["channel_id"], row["video_id"], row["video_name"], row["video_description"], row["published_date"], row["view_count"], row["like_count"], row["favorite_count"], row["comment_count"], row["duration"], row["thumbnail"])
            cursor.execute(sql, val)
            mydb.commit()
    except:
        print("Video Data are already inserted into MySQL DB")


#comment table
def comment_tb(channel_id):
    mydb = db.connect(
        host='localhost',
        user='enter your username',
        password='enter your password'
        database='youtube')
    cursor = mydb.cursor()
    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS comment (\
            comment_id VARCHAR(255)PRIMARY KEY,\
            video_id VARCHAR(255),\
            comment_text TEXT,\
            comment_author VARCHAR(255),\
            comment_published_date DATETIME\
        )")
        comment_info=get_comment_info(channel_id)
        comment_data=pd.DataFrame(comment_info)
        for index, row in comment_data.iterrows():
            sql = "INSERT INTO comment (comment_id, video_id, comment_text, comment_author, comment_published_date) VALUES (%s, %s, %s, %s, %s)"
            val = (row["comment_id"], row["video_id"], row["comment_text"], row["comment_author"], row["comment_published_date"])
            cursor.execute(sql, val)
            mydb.commit()
    except:
        print("Comment Data are already inserted into MySQL DB")


#function for storing channel details in tables
def mysql_table():
    try:
        channel_tb(channel_id)
        playlist_tb(channel_id)
        video_tb(channel_id)
        comment_tb(channel_id)
        return ("Tables are created and channel details are stored succesfully")
    except:
        pass


# Set Streamlit app 
all_channel_ids=[]
st.set_page_config(layout="wide")

with st.sidebar:
    
    st.header(":red[Skills Required]")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("Streamlit")
    st.caption("API integration")
    st.caption("MySQL Database")

st.title(":blue[YouTube Data Harvesting and Warehousing]")

channel_id=st.text_input("Enter the channel ID:","")


if st.button("Fetch Data"):
    st.header("Channel details")
    st.subheader("Channel Data:")
    channel_data = get_channel_info(channel_id)
    st.write(channel_data)
    st.subheader("Playlist Data")
    playlist_data = get_playlist_details(channel_id)
    st.write(playlist_data)
    st.subheader("Video Id Data")
    videoid_data = get_video_ids(channel_id)
    st.write(videoid_data)
    st.subheader("Video Data")
    video_data = get_video_info(channel_id)
    st.write(video_data)
    st.subheader("Comment Data")
    comment_data = get_comment_info(channel_id)
    st.write(comment_data)
        
if st.button("Store data to MySQL DB"):
    table=mysql_table()
    st.success(table)

#MySQL connection
mydb = db.connect(
        host='localhost',
        user='enter your username',
        password='enter your password'
        database='youtube')
cursor = mydb.cursor()


# SQL Query options
query_option = st.selectbox('Select SQL Query Option',
                            ['1.Video and Channel Names', '2.Channels with Most Videos',
                             '3.Top 10 Most Viewed Videos', '4.Comments Count by Video',
                             '5.Videos with Highest Likes', '6.Likes by Video',
                             '7.Total Views by Channel', '8.Channels with Videos in 2022',
                             '9.Average Video Duration by Channel', '10.Videos with Most Comments'])
if st.button("View"):

    # Execute SQL Query based on user selection
    if query_option == '1.Video and Channel Names':
            
        query1 = "SELECT video_name, channel_name FROM video"
        cursor.execute(query1)
        c1=cursor.fetchall()
        df1=pd.DataFrame(c1,columns=["Video Title","Channel Name"])
        st.write(df1)

    elif query_option == '2.Channels with Most Videos':
        
        query2 = "SELECT channel_name, total_videos AS video_count FROM channels  ORDER BY video_count DESC"
        cursor.execute(query2)
        c2=cursor.fetchall()
        df2=pd.DataFrame(c2,columns=["Channel Name","No.of Videos"])
        st.write(df2)
        fig2 = go.Figure(data=[go.Bar(x=df2['No.of Videos'], y=df2['Channel Name'], orientation='h')])
        fig2.update_layout(title='Channels with Most Videos', xaxis_title='Number of Videos', yaxis_title='Channel Name')
        st.plotly_chart(fig2)


    elif query_option == '3.Top 10 Most Viewed Videos':
        
        query3 = "SELECT view_count, channel_name, video_name  FROM video where view_count is not null ORDER BY view_count DESC LIMIT 10"
        cursor.execute(query3)
        c3=cursor.fetchall()
        df3=pd.DataFrame(c3,columns=["Views","Channel Name","Video Title"])
        st.write(df3)
        fig3 = go.Figure(data=[go.Bar(y=df3['Video Title'], x=df3['Views'], orientation='h')])
        fig3.update_layout(title='Top 10 Most Viewed Videos', xaxis_title='Views', yaxis_title='Video Title')
        st.plotly_chart(fig3)

    elif query_option == '4.Comments Count by Video':
        
        query4 = "SELECT comment_count, video_name FROM video where comment_count is not null"
        cursor.execute(query4)
        c4=cursor.fetchall()
        df4=pd.DataFrame(c4,columns=["No.of Comments","Video Title"])
        st.write(df4)
        
    elif query_option == '5.Videos with Highest Likes':
        
        query5 = "SELECT video_name, channel_name, like_count FROM video where like_count is not null ORDER BY like_count DESC"
        cursor.execute(query5)
        c5=cursor.fetchall()
        df5=pd.DataFrame(c5,columns=["Video Title","Channel Name","Like Count"])
        st.write(df5)
        
    elif query_option == '6.Likes by Video':
        
        query6 = "select video_name, like_count from video where like_count is not null"
        cursor.execute(query6)
        c6=cursor.fetchall()
        df6=pd.DataFrame(c6,columns=["Videos Title","Likes Count"])
        st.write(df6)
        

    elif query_option == '7.Total Views by Channel':
        
        query7 = "SELECT channel_name, channel_views FROM channels"
        cursor.execute(query7)
        c7=cursor.fetchall()
        df7=pd.DataFrame(c7,columns=["Channel name","Total views"])
        st.write(df7)
        fig7 = go.Figure(data=[go.Bar(x=df7['Channel name'], y=df7['Total views'])])
        fig7.update_layout(title='Total Views by Channel', xaxis_title='Channel Name', yaxis_title='Total Views')
        st.plotly_chart(fig7)

    elif query_option == '8.Channels with Videos in 2022':
        
        query8 = "SELECT video_name,published_date, channel_name FROM video WHERE YEAR(published_date) = 2022"
        cursor.execute(query8)
        c8=cursor.fetchall()
        df8=pd.DataFrame(c8,columns=["Video Title","Published Date","Channel Name"])
        st.write(df8)
        fig8 = px.scatter(df8, x="Published Date", y="Channel Name")
        fig8.update_layout(title='Channels with Videos in 2022', xaxis_title='Published Date', yaxis_title='Channel Name')
        st.plotly_chart(fig8)

    elif query_option == '9.Average Video Duration by Channel':
        
        query9 = "SELECT channel_name, AVG(TIME_TO_SEC(duration)) AS average_duration_in_seconds FROM video GROUP BY channel_name"
        cursor.execute(query9)
        c9 = cursor.fetchall()
        df9 = pd.DataFrame(c9, columns=["Channel name", "Average Duration (in seconds)"])
        average_durations_seconds = df9['Average Duration (in seconds)'].astype('int')
        average_durations_timedelta = pd.to_timedelta(average_durations_seconds, unit='s')
        df9['Average Duration'] = average_durations_timedelta
        st.write(df9)

    elif query_option == '10.Videos with Most Comments':
        
        query10 = "SELECT video_name, channel_name, comment_count FROM video where comment_count is not null ORDER BY comment_count DESC "
        cursor.execute(query10)
        c10=cursor.fetchall()
        df10=pd.DataFrame(c10,columns=["Video Title","Channel Name","Comments"])
        st.write(df10)





