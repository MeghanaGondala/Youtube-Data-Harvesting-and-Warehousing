
from googleapiclient.discovery import build
import streamlit as st
import pandas as pd
import pymongo
import mysql.connector
import json

 
def api_connect():
    api_key='AIzaSyC5z5cTx5T6AxVmgBb9FmYZcz1TjQFvXc8'
    youtube=build('youtube','v3',developerKey=api_key)
    return youtube
youtube=api_connect()

def get_channel_details(channel_id):
    request = youtube.channels().list(
                      part="snippet,contentDetails,statistics",
                      id=channel_id
    )
    response = request.execute()

    for item in response['items']:
        data=dict(channel_title=item['snippet']['title'],
              channel_Id=item['id'],
              channel_description=item['snippet']['description'],
              published_at=item['snippet']['publishedAt'],
              subscriber_count=item['statistics']['subscriberCount'],
              view_count=item['statistics']['viewCount'],
              video_count=item['statistics']['videoCount'],
              playlist_Id=item['contentDetails']['relatedPlaylists']['uploads']
        )
    return data


def get_videos_ids(channel_id):
    videos_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlists_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
            part='snippet',
            playlistId=Playlists_Id,
            maxResults=50,
            pageToken=next_page_token).execute()
        
         
        for i in range(len(response1['items'])):
            videos_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')   
        
        if next_page_token is None:
            break

    return videos_ids
    

def get_video_details(video_ids):
                 video_data=[]
                 for video_id in video_ids:
                     request=youtube.videos().list(
                             part='snippet,contentDetails,statistics',
                             id=video_id)
                     
                     response=request.execute()

                    
                     for item in response['items']:
                         data=dict(channel_title=item['snippet']['channelTitle'],
                                   channel_id=item['snippet']['channelId'],
                                   video_Id=item['id'],
                                   Title=item['snippet']['title'],
                                   tags=item['snippet'].get('tags'),
                                   Description=item['snippet'].get('description'),
                                   Published_Date=item['snippet']['publishedAt'],
                                   Duration=item['contentDetails']['duration'],
                                   views=item['statistics'].get('viewCount'), 
                                   Comments=item['statistics'].get('commentCount'),
                                   likecount=item['statistics'].get('likeCount'),
                                   Favorite_Count=item['statistics']['favoriteCount'],
                                   Definition=item['contentDetails']['definition'],
                                   Caption_Status=item['contentDetails']['caption'],
                                   Thumbnail=item['snippet']['thumbnails']
                                       )
                         video_data.append(data)            
                                                         
                 return video_data             


def get_comment_details(video_ids):
    comment_data=[]
    try:
        
        
            for video_id in video_ids:
                request=youtube.commentThreads().list(
                  part='snippet',
                  videoId=video_id,
                  maxResults=50,
                  
                  )
                response=request.execute()
            

                for item in response['items']:
                    data=dict(
                      comment_id=item['snippet']['topLevelComment']['id'],
                      video_id=item['id'],
                      comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                      comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                      comment_published=item['snippet']['topLevelComment']['snippet']['publishedAt']
                      )
                    comment_data.append(data)
                
                
                  
                  

    except:
        pass
    return comment_data        


def get_playlists_details(channel_id):
    next_page_token= None
    all_data=[]
    while True:
        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId= channel_id,
            maxResults=50,
            pageToken=next_page_token

        )
        response=request.execute()

        for item in response['items']:
            data=dict(playlists_id=item['id'],
                      title=item['snippet']['title'],
                      channel_id=item['snippet']['channelId'],
                      channel_name=item['snippet']['channelTitle'],
                      published_at=item['snippet']['publishedAt'],
                      video_count=item['contentDetails']['itemCount'])
            all_data.append(data)
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return all_data


con=pymongo.MongoClient("mongodb://localhost:27017")
db=con['Youtube']

def insert_channel_mdb(channel_id):
    db=con['Youtube']
    col1=db['channel_details']
    try:
      if channel_id:
         existing_channel=col1.find_one({'channel_details.channel_Id':channel_id})
         if existing_channel:
            print('channel already exists')
         else:
            ch_details=get_channel_details(channel_id)
            c={"channel_details": ch_details}
            col1.insert_one(c)
            col2=db['video_details']
            video_ids=get_videos_ids(channel_id)
            video_details=get_video_details(video_ids)       
            b={"video_details": video_details}
            col2.insert_one(b)
            col3=db['comment_details']
            comment_details=get_comment_details(video_ids)
            a={"comment_details": comment_details}
            col3.insert_one(a)
            col4=db['playlists_details']
            playlists_details=get_playlists_details(channel_id)
            col4.insert_one({"playlists_details":playlists_details})
            print('channel uploaded to mongodb successfully')
         
      else:
         print('channel Id is null or empty')
    except Exception as e:
         print(f'Error ocurred while inserting channel data:{e}')


#creating channels table
import pandas as pd
import mysql.connector
def channels_table(channel_id):
    mydb=mysql.connector.connect(host='localhost',
                                 user='root',
                                 password='@Meghana15',
                                 database='Youtube'
                                )
    cursor=mydb.cursor()

   
    
    try:
        sql='''create table if not exists channels(channel_title varchar(100),
                                                   channel_Id varchar(80) primary key,
                                                   channel_description text,
                                                   published_at varchar(100),
                                                   subscriber_count bigint,
                                                   view_count bigint,
                                                   video_count int,
                                                   playlist_Id varchar(80) 
                                                   )'''
        cursor.execute(sql)
        mydb.commit()


    except:
        print('Channnels table already created')

    
    channels_list=[]
    db=con['Youtube']
    col1=db['channel_details']
    d = col1.find_one({'channel_details.channel_Id':channel_id},{'_id':0})
    df=pd.DataFrame(d["channel_details"],index=[0])
    
    for index,row in df.iterrows():
        sql='''insert into channels(channel_title ,
                                    channel_Id,
                                    channel_description,
                                    published_at,
                                    subscriber_count,
                                    view_count,
                                    video_count,
                                    playlist_Id)
                                        
                                    values(%s,%s,%s,%s,%s,%s,%s,%s)'''
        
        values=(row['channel_title'],
                row['channel_Id'],
                row['channel_description'],
                row['published_at'],
                row['subscriber_count'],
                row['view_count'],
                row['video_count'],
                row['playlist_Id'],
               )
    try:
        cursor.execute(sql,values)
        mydb.commit()
        
    except:
        print('Channel details already inserted into sql') 

# creating videos table
import pandas as pd
import mysql.connector
import json
from datetime import datetime, timedelta
import re
def convert_duration(Duration):
    # (\d+) - one or more digits.
    # ? - makes the preceding element or group in the pattern optional
    # PT,M,S are alphabets as mentioned in the expression inbetween which digits are present
    # PT(hrs)H(mins)M(secs)S
    duration_regex = r'PT((\d+)H)?((\d+)M)?((\d+)S)?'
    matches = re.match(duration_regex, Duration)
    # Period of Time timestamp(string) to seconds(int)
    if matches:
        # In this pattern, the first group starts with "(", so it's the number 1.
        # The second group starts with "((", so it's number 2
        # The third group starts after ? & so, it goes on
        hours = int(matches.group(2) or 0)
        minutes = int(matches.group(4) or 0)
        seconds = int(matches.group(6) or 0)
        total_seconds = hours * 3600 + minutes * 60 + seconds
        return total_seconds

    return 0

def convert_published_date(Published_Date_str):
    # Assuming published_date_str is in a specific format, adjust as needed
    return datetime.strptime(Published_Date_str, "%Y-%m-%dT%H:%M:%SZ")



def videos_table(channel_id):
    
        mydb=mysql.connector.connect(host='localhost',
                                 user='root',
                                 password='@Meghana15',
                                 database='Youtube'
                                )
        cursor=mydb.cursor()
        
        try:
            query='''create table if not exists videos(channel_title varchar(100),
                                             channel_id varchar(80),
                                             video_Id varchar(30) primary key,
                                             Title varchar(150),
                                             tags text,
                                             Description text,
                                             Published_Date datetime,
                                             Duration int,
                                             views int, 
                                             Comments int,
                                             likecount int,
                                             Favorite_Count int,
                                             Definition varchar(10),
                                             Caption_Status varchar(50),
                                             Thumbnail text
                                             )'''
                                                
            cursor.execute(query)
            mydb.commit()
    
    

            videos_list=[]
            db=con['Youtube']
            col2=db['video_details']
            for video_data in col2.find({'video_details.channel_id':channel_id},{'_id':0}):
                for i in range(len(video_data['video_details'])):
                     videos_list.append(video_data['video_details'][i])
      
            df1=pd.DataFrame(videos_list)
    

            for index,row in df1.iterrows():
                 # Convert Duration to seconds
                duration_seconds = convert_duration(row['Duration'])

                 # Convert published_date to datetime
                Published_Date = convert_published_date(row['Published_Date'])

                query='''insert into videos(channel_title,
                                  channel_id,
                                  video_Id,
                                  Title,
                                  tags,
                                  Description,
                                  Published_Date,
                                  Duration,
                                  views, 
                                  Comments,
                                  likecount,
                                  Favorite_Count,
                                  Definition,
                                  Caption_Status,
                                  Thumbnail)
                                  values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                  '''
                values=(row['channel_title'],
                    row['channel_id'],
                    row['video_Id'],
                    row['Title'],
                    json.dumps(row['tags']),
                    row['Description'],
                    Published_Date,
                    duration_seconds,
                    
                    row['views'],
                    row['Comments'],
                    row['likecount'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_Status'],
                    json.dumps(row['Thumbnail'])
                    )
                cursor.execute(query,values)
                mydb.commit()


        except Exception as e:
            print(f"Error: {e}")

    

#creating comments table
def comments_table():
    mydb= mysql.connector.connect(host='localhost',
                                  user='root',
                                  password='@Meghana15',
                                  database='Youtube')
    cursor=mydb.cursor()


    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()

    sql = '''create table if not exists comments(comment_id varchar(100) primary key,
                                                 video_id varchar(50),
                                                 comment_text text, 
                                                 comment_author varchar(150),
                                                 comment_published varchar(200))'''
    cursor.execute(sql)
    mydb.commit()


    comments_list=[]
    db=con['Youtube']
    col3=db['comment_details']
    for comment_data in col3.find({},{"_id":0}):
        for i in range(len(comment_data['comment_details'])):
            comments_list.append(comment_data['comment_details'][i])

    df2=pd.DataFrame(comments_list)


    for index,row in df2.iterrows():
        sql = '''insert into comments(comment_id ,
                                      video_id ,
                                      comment_text, 
                                      comment_author ,
                                      comment_published)
                                      
                                      values(%s,%s,%s,%s,%s)'''
        values=(row['comment_id'] ,
               row['video_id'] ,
               row['comment_text'], 
               row['comment_author'] ,
               row['comment_published'])
        
        cursor.execute(sql,values)
        mydb.commit()

    
def show_channels_table():
    channels_list=[]
    db=con['Youtube']
    col1=db['channel_details']
    for ch_data in col1.find({},{'_id':0}):
        channels_list.append(ch_data['channel_details'])
      
    df=st.dataframe(channels_list)

    return df

def show_videos_table():
    videos_list=[]
    db=con['Youtube']
    col2=db['video_details']
    for video_data in col2.find({},{'_id':0}):
        for i in range(len(video_data['video_details'])):
            videos_list.append(video_data['video_details'][i])
      
    df1=st.dataframe(videos_list)

    return df1

def show_comments_table():
    comments_list=[]
    db=con['Youtube']
    col3=db['comment_details']
    for comment_data in col3.find({},{"_id":0}):
        for i in range(len(comment_data['comment_details'])):
            comments_list.append(comment_data['comment_details'][i])

    df2=st.dataframe(comments_list)


    return df2
    

    
with st.sidebar:
    st.title(':rainbow[YOUTUBE DATA HARVESTING AND WAREHOUSING]')
    st.header(':blue[Key Takeaways]',divider='rainbow')
    st.caption(':orange[Python Scripting]')
    st.caption(':violet[Data Collection]')
    st.caption(':red[MongoDB]')
    st.caption('API Integration')
    st.caption('Data Management using MongoDB and SQL')

channel_id=st.text_input('Enter the channel ID')

if st.button('collect and store data'):
    ch_ids=[]
    db=con['Youtube']
    col1=db['channel_details']
    for ch_data in col1.find({},{'_id':0}):
        ch_ids.append(ch_data['channel_details']['channel_Id'])

    if channel_id in ch_ids:
        st.success('Channels details of given channel id already exists')

    else:
        insert=insert_channel_mdb(channel_id)
        st.success(insert)
        st.success("Channel details inserted into mongodb sucsessfully")

if st.button('Migrate into SQL'):
    channels_table(channel_id)
    videos_table(channel_id)
    comments_table()
    st.success("Tables creation successful")

show_table=st.radio('Select the table for view',('CHANNELS','VIDEOS','COMMENTS'))


if show_table=='CHANNELS':
        show_channels_table()


elif show_table=='VIDEOS':
        show_videos_table()

elif show_table=='COMMENTS':
        show_comments_table()

    
mydb= mysql.connector.connect(host='localhost',
                                  user='root',
                                  password='@Meghana15',
                                  database='Youtube')
cursor=mydb.cursor()


question=st.selectbox('Select your question',('1.All the videos and the channel name',
                                               '2.Channels with most number of videos',
                                               '3.10 most viewed videos',
                                               '4.Comments in each videos',
                                               '5.Videos with highest likes',
                                               '6.Likes of all videos',
                                               '7.Views of each channel',
                                               '8.Videos published in the year of 2022',
                                               '9.Average duration of all videos in each channel',
                                               '10.Videos with highest number of comments',))


if question=='1.All the videos and the channel name':
    query1='''select Title as videostitle,channel_title as channelname from videos'''
    cursor.execute(query1)
    t1=cursor.fetchall()
    df1=pd.DataFrame(t1,columns=['video title','channel name'])
    st.write(df1)


elif question=='2.Channels with most number of videos':
    query2='''select channel_title as channelname,video_count as no_videos from channels order by video_count desc'''
    cursor.execute(query2)
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=['channel name','No of videos'])
    st.write(df2)


elif question=='3.10 most viewed videos':
    query3='''select views as views,channel_title as channelname,Title as videotitle from videos where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=['views','channel name','video title'])
    st.write(df3)


elif question=='4.Comments in each videos':
    query4='''select Comments as no_comments,Title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=['No of comments','video title'])
    st.write(df4)


elif question=='5.Videos with highest likes':
    query5='''select Title as videotitle,channel_title as channelname,likecount as likecount from videos where likecount is not null order by likecount desc'''
    cursor.execute(query5)
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=['video title','channel name','likecount'])
    st.write(df5)


elif question=='6.Likes of all videos':
    query6='''select likecount as likecount,Title as videotitle from videos'''
    cursor.execute(query6)
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=['likecount','video title'])
    st.write(df6)


elif question=='7.Views of each channel':
    query7='''select channel_title as channelname,view_count as totalviews from channels'''
    cursor.execute(query7)
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=['channel name','total views'])
    st.write(df7)


elif question=='8.Videos published in the year of 2022':
    query8='''select Title as video_title,Published_Date as videorelease,channel_title as channelname from videos where extract(year from published_date)=2022'''
    cursor.execute(query8)
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=['videotitle','published_date','channel name'])
    st.write(df8)


elif question=='9.Average duration of all videos in each channel':
    query9='''select channel_title as channelname,SEC_TO_TIME(AVG(Duration)) as average_duration from videos group by channel_title'''
    cursor.execute(query9)
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname",'averageduration'])
    
    T9=[]
    for index,row in df9.iterrows():
        channel_title=row['channelname']
        average_duration=row['averageduration']
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,average_duration=average_duration_str))
    df9=pd.DataFrame(T9)
    st.write(df9)
    


elif question=='10.Videos with highest number of comments':
     query10='''select Title as videotitle,channel_title as channelname,comments as comments from videos where comments is not null order by comments desc'''
     cursor.execute(query10)
     t10=cursor.fetchall()
     df10=pd.DataFrame(t10,columns=['videotitle','channel name','comments'])
     st.write(df10)
    
    


                                              