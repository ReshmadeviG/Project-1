import googleapiclient.discovery #youtube API
import googleapiclient.errors  #youtube API
import pymongo  #MongoDB
from pymongo.mongo_client import MongoClient #MongoDB
import mysql.connector #Sql connector
import pandas as pd #Sql
import streamlit as st #streamlit

#function to get channel details

    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.

#API Key

api_service_name = "youtube"
api_version = "v3"
    

    # Get credentials and create an API client
youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey="AIzaSyBgkypsOh-VXBpAhxeZVaD6iEE35GsKQx8")

#step1: data extraction of channels, Playlist, videos and comments

#function to get channel data

def get_information(channel_id):

    request = youtube.channels().list(
         part="snippet,contentDetails,statistics",
         id=channel_id
         )
    response = request.execute()

    for i in response['items']:
        data = dict(Channel_Name=i["snippet"]["title"],
                    Channel_ID=i["id"],
                    Channel_Subscriber=i["statistics"]["subscriberCount"],
                    videocount=i["statistics"]["videoCount"],
                    Viewscount=i["statistics"]["viewCount"],
                    description=i["snippet"]["description"],
                    PlaylistID=i["contentDetails"]["relatedPlaylists"]["uploads"])
        return data
    
 # function to get the playlist details

def get_playlist_info(channel_id):

    next_page_token=None
    playlistinfo=[]

    while True:
        request = youtube.playlists().list(part="snippet,contentDetails",
                                           channelId=channel_id,
                                           maxResults=50,
                                           pageToken=next_page_token)
        response = request.execute()

        for i in response["items"]:
            playlistdata=dict(channelid=i["snippet"]["channelId"],
                              channelname=i["snippet"]["channelTitle"],
                              playlistid=i["id"],
                              playlistname=i["snippet"]["title"],
                              video_count=i["contentDetails"]["itemCount"])
            playlistinfo.append(playlistdata)

        next_page_token = response.get('nextPageToken')

        if next_page_token is None:
                break
                
    return playlistinfo

#function to get video ids

def get_video_ids(channel_id):
    full_videos=[]
    request = youtube.channels().list(part="contentDetails",
                                      id=channel_id).execute()
    playlistid = request['items'][0]['contentDetails']['relatedPlaylists']['uploads'] 

    next_page_token = None
    while True:
        videoid = youtube.playlistItems().list(part='snippet',
                                               playlistId=playlistid,
                                               maxResults=50,
                                               pageToken = next_page_token).execute()
        for i in range(len(videoid['items'])):
            full_videos.append(videoid['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = videoid.get('nextPageToken')
    
        if next_page_token is None:
            break
    return full_videos


#function to get video information

def get_video_infor(video_ids):
    video_information=[]
    for video_infor in video_ids:
        request=youtube.videos().list(part = "snippet,contentDetails,statistics",
                                      id =video_infor).execute()
        for details in request["items"]:
            data=dict(Channel_Name=details['snippet']['channelTitle'],
                      Channel_id=details['snippet']['channelId'],
                      Video_id=details['id'],
                      Video_Name=details['snippet']['title'],
                      Video_description=details['snippet'].get('description'),
                      Tags=details['snippet'].get('tags'),
                      PublishedAt=details['snippet']['publishedAt'],
                      View_count=details['statistics'].get('viewCount'),
                      like_count=details['statistics'].get('likeCount'),
                      Favourite_count=details['statistics'].get('favoriteCount'),
                      Comment_count=details['statistics'].get('commentCount'),
                      Duration=details['contentDetails'].get('duration'),
                      thumbnail=details['snippet']['thumbnails']['default']['url'],
                      CaptionStatus=details['contentDetails']['caption']
                      )
            video_information.append(data)
    return video_information

# function to get the comment details

def get_comment_details(video_ids):
    comment_info=[]
    try:
        for commentdetails in video_ids:
            request=youtube.commentThreads().list(part="snippet",
                                                  videoId = commentdetails,
                                                  maxResults=50).execute()
            for details in request["items"]:
                commentdata = dict(video_id=details["snippet"]["topLevelComment"]["snippet"]["videoId"],
                                   comment_id = details["snippet"]["topLevelComment"]["id"],
                                   comment_text=details["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                   comment_author=details["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                   comment_publishedat=details["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                comment_info.append(commentdata)

    except:
        pass
    
    return comment_info

#Step 2 : extracting the data to MongoDB

client = MongoClient("mongodb+srv://reshmadevig32:1234@cluster0.j5gu8fk.mongodb.net/?retryWrites=true&w=majority")
db=client.youtube_project
records = db.extractdetails

def extract_channeldetails(channel_id):
    channeldetails=get_information(channel_id)
    playlistdetail=get_playlist_info(channel_id)
    video_ids=get_video_ids(channel_id)
    videoinformation=get_video_infor(video_ids)
    comment_detail=get_comment_details(video_ids)
    
    records.insert_one({"channel":channeldetails,"playlist":playlistdetail,"videoid":video_ids,
                        "video":videoinformation,"commentdetails":comment_detail})
    return "extracted the channel data"

# Step 3: extracting data to SQL to create separate tables
# SQL table for channel details

def channel_table():

    mydb = mysql.connector.connect(host="localhost",
                                   user="root",
                                   password="")

    print(mydb)

    try:
        mycursor.execute("create database youtube_project")
    except:
        print("database already created")
        
    
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute("USE youtube_project")
    drop_query = """drop table if exists channels_details"""
    mycursor.execute(drop_query)
    mydb.commit()
    

    try:
        create_channel = """create table if not exists channels_details(Channel_Name varchar(255),
                                                                        Channel_ID varchar(255) primary key,
                                                                        Channel_Subscriber integer,
                                                                        videocount integer,
                                                                        Viewscount integer,
                                                                        description longtext,
                                                                        PlaylistID varchar(255))"""
        mycursor.execute(create_channel)
        mydb.commit()

    except:
        print("Channels data already created")

    channel_list=[]
    db=client["youtube_project"]
    records= db["extractdetails"]
    for channel_table in records.find({},{"_id":0,"channel":1}):
        channel_list.append(channel_table["channel"])
    df=pd.DataFrame(channel_list)

    for index,row in df.iterrows():
        Channeldata = '''insert into channels_details(Channel_Name,
                                                      Channel_ID,
                                                      Channel_Subscriber,
                                                      videocount,
                                                      Viewscount,
                                                      description,
                                                      PlaylistID)
                                                      values(%s,%s,%s,%s,%s,%s,%s)'''

        values=(row['Channel_Name'],
                row['Channel_ID'],
                row['Channel_Subscriber'],
                row['videocount'],
                row['Viewscount'],
                row['description'],
                row['PlaylistID'])

        try:
            mycursor.execute(Channeldata,values)
            mydb.commit()

        except:
            print("Channel name already inserted")

# SQL table for Playlist details

def playlist_table():
    mydb = mysql.connector.connect(host="localhost",
                                       user="root",
                                       password="")

    print(mydb)

    try:
        mycursor.execute("create database youtube_project")
    except:
        print("database already created")


    mycursor = mydb.cursor(buffered=True)
    mycursor.execute("USE youtube_project")
    drop_query = """drop table if exists playlists"""
    mycursor.execute(drop_query)
    mydb.commit()

    create_channel = """create table if not exists playlists(playlistid varchar(255) primary key,
                                                             playlistname varchar(100),
                                                             channelid varchar(255),
                                                             channelname varchar(255),
                                                             video_count integer)"""


    mycursor.execute(create_channel)
    mydb.commit()

    playlist_list=[]
    db=client["youtube_project"]
    records= db["extractdetails"]
    for Playlist_table in records.find({},{"_id":0,"playlist":1}):
        for i in range(len(Playlist_table["playlist"])):
            playlist_list.append(Playlist_table["playlist"][i])
    df1=pd.DataFrame(playlist_list)

    for index,row in df1.iterrows():
        Playlist_data = '''insert into playlists(playlistid,
                                                 playlistname,
                                                 channelid,
                                                 channelname,
                                                 video_count) 
                                                 values(%s,%s,%s,%s,%s)'''

        values=(row['playlistid'],
                row['playlistname'],
                row['channelid'],
                row['channelname'],
                row['video_count'])


        mycursor.execute(Playlist_data,values)
        mydb.commit()

# SQL table for Video details

def video_table():
    mydb = mysql.connector.connect(host="localhost",
                                       user="root",
                                       password="")

    print(mydb)

    try:
        mycursor.execute("create database youtube_project")
    except:
        print("database already created")


    mycursor = mydb.cursor(buffered=True)
    mycursor.execute("USE youtube_project")
    drop_query = """drop table if exists videos"""
    mycursor.execute(drop_query)
    mydb.commit()


    create_channel = """create table if not exists videos(Channel_Name varchar(255),
                                                          Channel_id varchar(255),
                                                          Video_id varchar(30),
                                                          Video_Name varchar(255),
                                                          Video_description text,
                                                          PublishedAt timestamp,
                                                          View_count integer,
                                                          like_count integer,
                                                          Favourite_count integer,
                                                          Comment_count integer,
                                                          Duration varchar(100),
                                                          thumbnail varchar(255),
                                                          CaptionStatus varchar(255))"""


    mycursor.execute(create_channel)
    mydb.commit()

    video_list=[]
    db=client["youtube_project"]
    records= db["extractdetails"]
    for video_table in records.find({},{"_id":0,"video":1}):
        for i in range(len(video_table["video"])):
            video_list.append(video_table["video"][i])
    df2=pd.DataFrame(video_list)

    for index,row in df2.iterrows():
        vi_data = '''insert into videos(Channel_Name,
                                           Channel_id,
                                           Video_id,
                                           Video_Name,
                                           Video_description,
                                           PublishedAt,
                                           View_count,
                                           like_count,
                                           Favourite_count,
                                           Comment_count,
                                           Duration,
                                           thumbnail,
                                           CaptionStatus)
                                           values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        values=(row['Channel_Name'],
                row['Channel_id'],
                row['Video_id'],
                row['Video_Name'],
                row['Video_description'],
                row['PublishedAt'],
                row['View_count'],
                row['like_count'],
                row['Favourite_count'],
                row['Comment_count'],
                row['Duration'],
                row['thumbnail'],
                row['CaptionStatus'])

        mycursor.execute(vi_data,values)
        mydb.commit()


# SQL table for Comment details

def comment_table():
    mydb = mysql.connector.connect(host="localhost",
                                       user="root",
                                       password="")

    print(mydb)

    try:
        mycursor.execute("create database youtube_project")
    except:
        print("database already created")


    mycursor = mydb.cursor(buffered=True)
    mycursor.execute("USE youtube_project")
    drop_query = """drop table if exists comments"""
    mycursor.execute(drop_query)
    mydb.commit()


    create_channel = """create table if not exists comments(video_id varchar(255),
                                                             comment_id varchar(100) primary key,
                                                             comment_text text,
                                                             comment_author varchar(255),
                                                             comment_publishedat timestamp)"""


    mycursor.execute(create_channel)
    mydb.commit()

    comment_list=[]
    db=client["youtube_project"]
    records= db["extractdetails"]
    for comment_table in records.find({},{"_id":0,"commentdetails":1}):
        for i in range(len(comment_table["commentdetails"])):
            comment_list.append(comment_table["commentdetails"][i])
    df3=pd.DataFrame(comment_list)

    for index,row in df3.iterrows():
        comment_data = '''insert into comments(video_id,
                                                comment_id,
                                                 comment_text,
                                                 comment_author,
                                                 comment_publishedat) 
                                                 values(%s,%s,%s,%s,%s)'''

        values=(row['video_id'],
                row['comment_id'],
                row['comment_text'],
                row['comment_author'],
                row['comment_publishedat'])

        mycursor.execute(comment_data,values)
        mydb.commit()

#Function to consolidating the tables(Channel, playlist, videos, Comments)

def full_table():
    channel_table()
    playlist_table()
    video_table()
    comment_table()
    
    return "Table inserted successfully"

full_table()

#This function helps to show all the tables in strealit 

def show_channel_data():
    channel_list=[]
    db=client["youtube_project"]
    records= db["extractdetails"]
    for channel_table in records.find({},{"_id":0,"channel":1}):
        channel_list.append(channel_table["channel"])
    df=st.dataframe(channel_list)
    return df

def show_playlist_data():
    playlist_list=[]
    db=client["youtube_project"]
    records= db["extractdetails"]
    for Playlist_table in records.find({},{"_id":0,"playlist":1}):
        for i in range(len(Playlist_table["playlist"])):
            playlist_list.append(Playlist_table["playlist"][i])
    df1=st.dataframe(playlist_list)
    return df1

def show_video_data():
    video_list=[]
    db=client["youtube_project"]
    records= db["extractdetails"]
    for video_table in records.find({},{"_id":0,"video":1}):
        for i in range(len(video_table["video"])):
            video_list.append(video_table["video"][i])
    df2=st.dataframe(video_list)
    return df2

def show_comment_data(): 
    comment_list=[]
    db=client["youtube_project"]
    records= db["extractdetails"]
    for comment_table in records.find({},{"_id":0,"commentdetails":1}):
        for i in range(len(comment_table["commentdetails"])):
            comment_list.append(comment_table["commentdetails"][i])
    df3=st.dataframe(comment_list)
    return df3

#Step 4 : Creating the portal to showcase all the datas using Streamlit library

with st.sidebar:
    st.title("YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit")
    st.header("Data Analysis")
    st.caption("1.Enter Channel ID in the text box")
    st.caption("2.Extract the Data in MongoDB")
    st.caption("3.Migarte the data to SQL")
    st.caption("4.Check all the details about the channel")
    st.caption("5.Compare the your datas with other channels using the ten questions listed")

channel_id=st.text_input("Please Enter the channel ID:")

if st.button("Extract the data to MongoDB"):
    ch_ids=[]
    db=client["youtube_project"]
    records= db["extractdetails"]
    for ch_data in records.find({},{"_id":0,"channel":1}):
        ch_ids.append(ch_data["channel"]["Channel_ID"])

    if channel_id in ch_ids:
        st.success("The given channel ID details already exists")

    else:
        insert = extract_channeldetails(channel_id)
        st.success(insert)
        st.balloons()

if st.button("Migrate the data to SQL"):
    Table=full_table()
    st.success(Table)
    st.balloons()

show_table= st.radio("select the table and view the channel details",("Channels","Playlists","Videos","Comments"))

if show_table=="Channels":
    show_channel_data()

elif show_table=="Playlists":
    show_playlist_data()

elif show_table=="Videos":
    show_video_data()

elif show_table=="Comments":
    show_comment_data()

#SQL Conncection

mydb = mysql.connector.connect(host="localhost",
                                    user="root",
                                    password="")
print(mydb)
try:
    mycursor.execute("create database youtube_project")
except:
    print("database already created")
mycursor = mydb.cursor(buffered=True)
mycursor.execute("USE youtube_project")

# Query to retrieve distinct channel names

query_get_channel_names = '''SELECT DISTINCT Channel_Name FROM videos'''
mycursor.execute(query_get_channel_names)
mydb.commit()
channel_name_result = mycursor.fetchall()
channel_name = [row[0] for row in channel_name_result]

# Add a special option for selecting all channels
special_option = "All"
channel_name_with_all = [special_option] + channel_name

# Use a conditional statement to handle the selection
selected_channel = st.selectbox("Select the Channel:", options=channel_name_with_all)

questions=st.selectbox("Select the question:",("Please select",
                                              "1.What  are the names  of all the videos  and  their  corresponding  channels?",
                                              "2.Which  channels  have the most number  of videos,  and  how many videos  do they have?",
                                              "3.What  are the top 10 most viewed  videos  and  their  respective channels?",
                                              "4.How many comments  were made  on each  video,  and  what  are their corresponding  video  names?",
                                              "5.Which  videos  have the highest  number  of likes, and  what  are their corresponding  channel  names?",
                                              "6.What  is the total  number  of likes for each  video,  and  what  are their  corresponding  video  names?",
                                              "7.What  is the total  number  of views  for each  channel,  and  what  are their corresponding  channel  names?",
                                              "8.What  are the names  of all the channels  that  have published  videos  in the year 2022?",
                                              "9.What  is the average duration  of all videos  in each  channel,  and  what  are their corresponding  channel  names?",
                                              "10. Which  videos  have the highest  number  of comments,  and  what  are their corresponding  channel  names?"
                                              ))


if questions == "1.What  are the names  of all the videos  and  their  corresponding  channels?":
    if selected_channel is "All":
        query1 = "select Video_Name, Channel_Name from youtube_project.videos;"
    else:
        query1 = f"""select Video_Name, Channel_Name from youtube_project.videos where Channel_Name = '{selected_channel}'"""
    mycursor.execute(query1)
    mydb.commit()
    t1=mycursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))

elif questions == "2.Which  channels  have the most number  of videos,  and  how many videos  do they have?":
    if selected_channel is "All": 
        query2 = """select Channel_Name, videocount from youtube_project.channels_details order by videocount desc;"""
    else:
        query2 = f"""select Channel_Name, videocount from youtube_project.channels_details where Channel_Name ='{selected_channel}' order by videocount desc"""
    mycursor.execute(query2)
    mydb.commit()
    t2=mycursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

elif questions == "3.What  are the top 10 most viewed  videos  and  their  respective channels?":
    if selected_channel is "All":
        query3 = """select View_count, Channel_Name, Video_Name from videos where View_count is not null order by View_count Desc Limit 10"""
    else:
        query3 = f"""select View_count, Channel_Name, Video_Name from videos where View_count is not null and Channel_Name ='{selected_channel}' order by View_count Desc Limit 10"""
    mycursor.execute(query3)
    mydb.commit()
    t3=mycursor.fetchall()
    st.write(pd.DataFrame(t3, columns=["View count","Channel Name","Video title"]))

elif questions == "4.How many comments  were made  on each  video,  and  what  are their corresponding  video  names?":
    if selected_channel is "All":
        query4 = """select Comment_count, Video_Name from videos;"""
    else:
        query4 = f"""select Comment_count, Video_Name from videos where Channel_Name = '{selected_channel}'"""
    mycursor.execute(query4)
    mydb.commit()
    t4=mycursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["Comment_count","Video title"]))

elif questions == "5.Which  videos  have the highest  number  of likes, and  what  are their corresponding  channel  names?":
    if selected_channel is "All":
        query5 = """select Channel_Name, Video_Name, like_count from videos order by like_count Desc;"""
    else:
        query5 = f"""select Channel_Name, Video_Name, like_count from videos where Channel_Name = '{selected_channel}' order by like_count Desc"""
    mycursor.execute(query5)
    mydb.commit()
    t5=mycursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["Channel_name", "Video_Name","Like_Count"]))

elif questions == "6.What  is the total  number  of likes for each  video,  and  what  are their  corresponding  video  names?":
    if selected_channel is "All":
        query6 = """select Channel_Name, Video_Name, like_count from videos;"""
    else:
        query6 = f"""select Channel_Name, Video_Name, like_count from videos where Channel_Name = '{selected_channel}'"""
    mycursor.execute(query6)
    mydb.commit()
    t6=mycursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["Channel_name", "Video_Name","Like_Count"]))

elif questions == "7.What  is the total  number  of views  for each  channel,  and  what  are their corresponding  channel  names?":
    if selected_channel is "All":
        query7 = """select Channel_Name, Viewscount from channels_details order by Viewscount Desc"""
    else:
        query7 = f"""select Channel_Name, Viewscount from channels_details where Channel_Name = '{selected_channel}' order by Viewscount Desc"""
    mycursor.execute(query7)
    mydb.commit()
    t7=mycursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["Channel_name", "Viewscount"]))

elif questions == "8.What  are the names  of all the channels  that  have published  videos  in the year 2022?":
    if selected_channel is "All":
        query8 = """select Video_Name, PublishedAt, Channel_Name from videos where extract(year from PublishedAt)=2022"""
    else:
        query8 = f"""select Video_Name, PublishedAt, Channel_Name from videos where extract(year from PublishedAt)=2022 and Channel_Name = '{selected_channel}'"""
    mycursor.execute(query8)
    mydb.commit()
    t8=mycursor.fetchall()
    st.write(pd.DataFrame(t8, columns=["Channelname","Video name", "PublishedAt"]))

elif questions == "9.What  is the average duration  of all videos  in each  channel,  and  what  are their corresponding  channel  names?":
    if selected_channel is "All":
        query9= """select Channel_Name, AVG(Duration) from videos group by Channel_Name"""
    else:
        query9= f"""select Channel_Name, AVG(Duration) from videos where Channel_Name = '{selected_channel}' group by Channel_Name"""
    mycursor.execute(query9)
    mydb.commit()
    t9=mycursor.fetchall()
    df9=pd.DataFrame(t9, columns=["Channelname","Averageduration"])
    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["Channelname"]
        average_duration=row["Averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,averageduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)

elif questions == "10. Which  videos  have the highest  number  of comments,  and  what  are their corresponding  channel  names?":
    if selected_channel is "All":
        query10 = """select Channel_Name, Video_Name, Comment_count from videos order by Comment_count desc"""
    else:
        query10 = f"""select Channel_Name, Video_Name, Comment_count from videos where Channel_Name = '{selected_channel}'order by Comment_count desc"""
    mycursor.execute(query10)
    mydb.commit()
    t10=mycursor.fetchall()
    st.write(pd.DataFrame(t10, columns=["Channel_name", "VideoName", "comment count"])) 