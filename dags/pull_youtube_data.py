import googleapiclient.discovery
import googleapiclient.errors
import pandas as pd
from kafka import KafkaProducer
import logging
import json

api_service_name = "youtube"
api_version = "v3"
DEVELOPER_KEY = "AIzaSyBpZFkRGraZY5rXVWm1WrHLiZ1I2rA6D6k"

youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=DEVELOPER_KEY)

#################### GET COMMENTS ##################
def getcomments(video):
  request = youtube.commentThreads().list(
      part="snippet",
      videoId=video,
      maxResults=10
  )

  comments = []
  response = request.execute()

  for item in response['items']:
      comment = item['snippet']['topLevelComment']['snippet']
      public = item['snippet']['isPublic']
      comments.append([
         # comment['authorDisplayName'],
          comment['publishedAt'],
          #comment['likeCount'],
          comment['textOriginal'],
         # comment['videoId'],
          #public
      ])

  while (1 == 1):
    try:
     nextPageToken = response['nextPageToken']
    except KeyError:
     break

    nextPageToken = response['nextPageToken']
    nextRequest = youtube.commentThreads().list(part="snippet", videoId=video, maxResults=100, pageToken=nextPageToken)
    response = nextRequest.execute()

    for item in response['items']:
      comment = item['snippet']['topLevelComment']['snippet']
      public = item['snippet']['isPublic']
      comments.append([
          #comment['authorDisplayName'],
          comment['publishedAt'],
          #comment['likeCount'],
          comment['textOriginal'],
          #comment['videoId'],
          #public
      ])

  df2 = pd.DataFrame(comments, columns=['updated_at', 'text'])
  return df2

################### SEARCH VIDEOS ####################
def search_videos(product_name):
    request = youtube.search().list(
        q=f"{product_name}",
        part="id",
        type="video",
        maxResults=2  # Vous pouvez ajuster le nombre de résultats
    )

    response = request.execute()
    video_ids = [item['id']['videoId'] for item in response['items']]
    return video_ids

################### COMMENTS FOR PRODUCT ##################
def get_comments_for_product(product_name):
    video_ids = search_videos(product_name)

    df = pd.DataFrame()
    for video_id in video_ids:
        df2 = getcomments(video_id)
        df = pd.concat([df, df2], ignore_index=True)
    
    #csv_filename = "comments_data.csv"
    #df.to_csv(csv_filename, index=False)

    return df

def stream_data(product_name):
   
    try:
        res = get_comments_for_product(product_name)

        for index, row in res.iterrows():
            yield (
                json.dumps(index),
                json.dumps({
                    "updated_at": row['updated_at'],
                    "text": row['text'],
                })
            )
        print("Les données sont envoyé")
    
    except Exception as e:
        logging.error(f'An error occured: {e}')




