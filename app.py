import streamlit as st
from datetime import datetime
import isodate
import googleapiclient.discovery 
import mysql.connector
from bson.objectid import ObjectId
import pandas as pd
import pymongo
import os
from dotenv import load_dotenv

load_dotenv()

# Connect to MongoDB
mongo_client = client=pymongo.MongoClient(os.getenv("MONGOKEY"))
mongo_db = mongo_client["Youtube"]
mongo_collection = mongo_db["channel"]

# Connect to MySQL
mysql_connection = mysql.connector.connect(
  host="localhost",
  user="root",
  password=os.getenv("MYSQLKEY"),
  database="Youtube"
)
mysql_cursor = mysql_connection.cursor()



st.header('Youtube Data Harvesting and Warehousing')

api_key=os.getenv("APIKEY")
youtube= googleapiclient.discovery.build('youtube',"v3",developerKey=api_key)

def ChannelDetail(cha):

        ch_request = youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=cha
            )
        ch_response = ch_request.execute()



        channel_details = dict(title=ch_response['items'][0]['snippet']['title'],
                            ch_id=ch_response['items'][0]['id'],

                            description=ch_response['items'][0]['snippet']['description'],

                            sub_count=int(ch_response['items'][0]['statistics']['subscriberCount']),
                            video_count=int(ch_response['items'][0]['statistics']['videoCount']),
                            view_count=int(ch_response['items'][0]['statistics']['viewCount']),
                            playlist_id=ch_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"])

        ch_id=ch_response['items'][0]['id']

        playlist_request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=ch_id,
                maxResults=50
            )
        playlist_response = playlist_request.execute()

        all_playlist=[]
        for i in range(len(playlist_response["items"])):

            playlists=dict(playlist_name=playlist_response['items'][i]['snippet']['title'],
                playlistid=playlist_response['items'][i]['id'],
                playlist_description=playlist_response['items'][i]['snippet']['description'],
                playlist_videoscount=int(playlist_response['items'][i]['contentDetails']['itemCount']),
                playlist_published=datetime.strptime(playlist_response['items'][i]['snippet']['publishedAt'],'%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d'))

            all_playlist.append(playlists)



        playlist_id=ch_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        pl_request = youtube.playlistItems().list(
                part="contentDetails",
                playlistId = playlist_id,
                maxResults = 50

            )
        pl_response = pl_request.execute()




        video_ids=[]

        next_page_token = None
        while True:


                pl_request = youtube.playlistItems().list(
                        part="contentDetails",
                        playlistId = playlist_id,
                        maxResults = 50,
                        pageToken = next_page_token)

                pl_response = pl_request.execute()


                for j in pl_response['items']:
                    video_ids.append(j['contentDetails']['videoId'])

                next_page_token = pl_response.get("nextPageToken")
                if not next_page_token :
                        break


        VIDEOS=[]

        for j in range(len(video_ids)):
            vd_request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_ids[j]
                )
            vd_response = vd_request.execute()


            try:
                com_request = youtube.commentThreads().list(
                        part='snippet,replies',
                        videoId=vd_response['items'][0]['id'],
                        textFormat="plainText",
                        maxResults=100
                        )
                com_response = com_request.execute() 
            
                comtt3=[]

                for i in range(len(com_response['items'])):
                    commentss=dict(vid_id=vd_response['items'][0]['id'],
                                comment_id=com_response['items'][i]['id'],
                                comment_text=com_response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                                author_name=com_response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                comment_date=datetime.strptime(com_response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'],'%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d'))

                    comtt3.append(commentss)
            
                videos=dict(vid_id=vd_response['items'][0]['id'],
                                video_name = vd_response['items'][0]['snippet']['title'],
                                video_duration_seconds=isodate.parse_duration(vd_response['items'][0]['contentDetails']['duration']).total_seconds(),

                                video_published = datetime.strptime(vd_response['items'][0]['snippet']['publishedAt'],'%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d'),
                                video_description = vd_response['items'][0]['snippet']['description'],
                                video_thumbnail=vd_response['items'][0]['snippet']['thumbnails'],
                                video_comments_count=int( vd_response['items'][0]['statistics']['commentCount']),
                                video_favorite_count=int( vd_response['items'][0]['statistics']['favoriteCount']),
                                video_like_count= int(vd_response['items'][0]['statistics']['likeCount']),
                                video_view_count= int(vd_response['items'][0]['statistics']['viewCount']),
                                comment=comtt3)

                VIDEOS.append(videos)

            except:

                videos=dict(vid_id=vd_response['items'][0]['id'],
                                video_name = vd_response['items'][0]['snippet']['title'],
                                video_duration_seconds=isodate.parse_duration(vd_response['items'][0]['contentDetails']['duration']).total_seconds(),

                                video_published = datetime.strptime(vd_response['items'][0]['snippet']['publishedAt'],'%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d'),
                                video_description = vd_response['items'][0]['snippet']['description'],
                                video_thumbnail=vd_response['items'][0]['snippet']['thumbnails'],
                                
                                video_favorite_count=int( vd_response['items'][0]['statistics']['favoriteCount']),
                                video_like_count= int(vd_response['items'][0]['statistics']['likeCount']),
                                video_view_count= int(vd_response['items'][0]['statistics']['viewCount']))

                VIDEOS.append(videos)

                print(len(VIDEOS))

        channel_info=dict(channel = channel_details,
                        playlists = all_playlist,
                        videos = VIDEOS)
        
        mongo_collection.insert_one(channel_info)
    
        return channel_info 

#Data Migration

def DataMigrate(id):
    objInstance = ObjectId(id)

    document = mongo_collection.find_one({"_id":objInstance})

    # pprint(document)

    #Iterate over each document in the collection and creating channel table
    # for document in mongo_collection.find():
    db = document["channel"]
    query = "INSERT INTO Channel (channel_id, channel_name, channel_views,channel_video_count, channel_description) VALUES (%s, %s,%s, %s, %s)"
    val = (db["ch_id"], db["title"], db["view_count"], db["video_count"], db["description"])
    mysql_cursor.execute(query, val)    


    #fetching data from videos field
    for field in ["videos"]:
            objects = document.get(field, [])
            
            # Iterate over the object in the videos field and creating video table
            for obj in objects:
                query2 = "INSERT INTO videos (video_id, video_name, channel_id, video_description, published_date, view_count, like_count, favourite_count, comment_count, duration) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                val2 = (obj["vid_id"], obj["video_name"], db["ch_id"], obj["video_description"], obj["video_published"], obj["video_view_count"], obj["video_like_count"], obj["video_favorite_count"], obj["video_comments_count"], obj["video_duration_seconds"])
                mysql_cursor.execute(query2, val2)

                #Iterate over the comment object in the video field and creating comment table
                for com in obj["comment"]:
                    query3 = "INSERT INTO comment (comment_id, video_id, comment_text, comment_author, comment_published_date) VALUES (%s,%s,%s,%s,%s)"
                    val3= (com["comment_id"], com["vid_id"], com["comment_text"], com["author_name"], com["comment_date"])
                    mysql_cursor.execute(query3, val3)        


    # #commit the changes to mysql database
    mysql_connection.commit()


 

option = st.selectbox('Select',('Data Collection','Migrate','Data Analysis'))

if option=="Data Collection":

    cha=st.text_input("Enter the Channel Id : ")
    if st.button('Scrape'):
        out = ChannelDetail(cha)
        st.write(out)
elif option=="Migrate":
        if st.button('Migrate to SQL'):
            #getting the last uploaded document and calling migrate function 
            last_document = mongo_collection.find().sort([("_id", -1)]).limit(1)
            if last_document:
                DataMigrate(last_document[0]["_id"])   
            st.success("Data migrated to SQL")    
elif option=="Data Analysis":
   
  
        st.subheader("Data Analysis")

        option=st.selectbox("Select a question : ",('1. What are the names of all the videos and their corresponding channels?',
                                                    '2. Which channels have the most number of videos, and how many videos do they have?',
                                                    '3.  What are the top 10 most viewed videos and their respective channels?',
                                                    '4.  How many comments were made on each video, and what are their corresponding video names?',
                                                    '5.  Which videos have the highest number of likes, and what are their corresponding channel names?',
                                                    '6.  What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                                    '7.  What is the total number of views for each channel, and what are their corresponding channel names?',
                                                    '8.  What are the names of all the channels that have published videos in the year  2022?',
                                                    '9.  What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                                    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))
        
        if option=='1. What are the names of all the videos and their corresponding channels?':
            mysql_cursor.execute('SELECT videos.video_name, channel.channel_name from videos INNER JOIN channel on videos.channel_id = channel.channel_id ORDER BY RAND() LIMIT 20;')
            result_1 = mysql_cursor.fetchall()
            df1 = pd.DataFrame(result_1, columns=['Video Name', 'Channel Name']).reset_index(drop=True)
            df1.index += 1
            st.dataframe(df1)
            

        elif option=='2. Which channels have the most number of videos, and how many videos do they have?':
            mysql_cursor.execute('SELECT channel_video_count as videos , channel_name as Channel from channel order by videos desc;')
            result_2 = mysql_cursor.fetchall()
            df2 = pd.DataFrame(result_2,columns=['Video Count','Channel Name']).reset_index(drop=True)
            df2.index += 1
            st.dataframe(df2)

        elif option=='3.  What are the top 10 most viewed videos and their respective channels?':
           mysql_cursor.execute('Select videos.view_count, videos.video_name, channel.channel_name from videos inner join channel on videos.channel_id = channel.channel_id order by videos.view_count desc limit 10;')
           result_3 = mysql_cursor.fetchall()
           df3 = pd.DataFrame(result_3,columns=['View count','Video Name', 'Channel Name']).reset_index(drop=True)
           df3.index += 1
           st.dataframe(df3)

        elif option=='4.  How many comments were made on each video, and what are their corresponding video names?':
            mysql_cursor.execute('select video_name as video, comment_count as total_comment from videos order by Rand();')
            result_4 = mysql_cursor.fetchall()
            df4 = pd.DataFrame(result_4,columns=['Video Name', 'Comment count']).reset_index(drop=True)
            df4.index += 1
            st.dataframe(df4)
    

        elif option=='5.  Which videos have the highest number of likes, and what are their corresponding channel names?':
            mysql_cursor.execute('select videos.video_name as Video_name, videos.like_count as Total_likes, channel.channel_name as Channel from videos inner join channel on videos.channel_id = channel.channel_id order by Total_likes desc limit 20;')
            result_5= mysql_cursor.fetchall()
            df5 = pd.DataFrame(result_5,columns=[ 'Video Name', 'Like count','Channel Name']).reset_index(drop=True)
            df5.index += 1
            st.dataframe(df5)

        elif option=='6.  What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
            st.caption(':red[Note: In November 2021, YouTube removed the public dislike count from all of its videos]')
            mysql_cursor.execute('select video_name as Video, like_count as Likes from videos order by  Rand();')
            result_6= mysql_cursor.fetchall()
            df6 = pd.DataFrame(result_6,columns=['Video Name', 'Like count']).reset_index(drop=True)
            df6.index += 1
            st.dataframe(df6)

        elif option=='7.  What is the total number of views for each channel, and what are their corresponding channel names?':
            mysql_cursor.execute('select channel_name, channel_views from channel;')
            result_7= mysql_cursor.fetchall()
            df7 = pd.DataFrame(result_7,columns=['Channel Name', 'Total number of views']).reset_index(drop=True)
            df7.index += 1
            st.dataframe(df7)

        elif option=='8.  What are the names of all the channels that have published videos in the year  2022?':
            mysql_cursor.execute('SELECT DISTINCT channel_name FROM channel INNER JOIN videos ON channel.channel_id = videos.channel_id WHERE YEAR(published_date) = 2022;')
            result_8= mysql_cursor.fetchall()
            df8 = pd.DataFrame(result_8,columns=['Channel Name']).reset_index(drop=True)
            df8.index += 1
            st.dataframe(df8)

        elif option=='9.  What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            mysql_cursor.execute('select channel.channel_name, avg(videos.duration) as Average_duration from videos inner join channel on videos.channel_id = channel.channel_id group by channel.channel_id;')
            result_9= mysql_cursor.fetchall()
            df9 = pd.DataFrame(result_9,columns=['Channel Name','Average duration of videos in seconds']).reset_index(drop=True)
            df9.index += 1
            st.dataframe(df9)

        elif option=='10. Which videos have the highest number of comments, and what are their corresponding channel names?':
            mysql_cursor.execute('select channel.channel_name as Channel, videos.video_name as Video, videos.comment_count as Total_Comment from videos inner join channel on videos.channel_id = channel.channel_id order by comment_count desc;')
            result_10= mysql_cursor.fetchall()
            df10 = pd.DataFrame(result_10,columns=['Channel Name','Video Name', 'Number of comments']).reset_index(drop=True)
            df10.index += 1
            st.dataframe(df10)
                 


   


    




