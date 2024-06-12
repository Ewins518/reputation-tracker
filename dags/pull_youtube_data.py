import googleapiclient.discovery
import googleapiclient.errors
import pandas as pd
import logging
import json

api_service_name = "youtube"
api_version = "v3"
DEVELOPER_KEY = "AIzaSyBpZFkRGraZY5rXVWm1WrHLiZ1I2rA6D6k"

coordinates = [[34.02, -6.83], [48.86, 2.35], [35.68, 139.76], [40.71, -74.01], [-26.2, 28.04]]
countries = ['Morocco','France','Japan','USA','South Africa']

youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=DEVELOPER_KEY)



def getcomments(video):
    comments = []
    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video,
            maxResults=50  # Increase to 50 to reduce the number of API calls
        )

        response = request.execute()

        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']
            comments.append([
                comment['publishedAt'],
                comment['textOriginal']
            ])

        while 'nextPageToken' in response and len(comments) < 20:  # Fetch until we have 20 comments
            nextPageToken = response['nextPageToken']
            nextRequest = youtube.commentThreads().list(
                part="snippet",
                videoId=video,
                maxResults=50,
                pageToken=nextPageToken
            )
            response = nextRequest.execute()

            for item in response['items']:
                comment = item['snippet']['topLevelComment']['snippet']
                comments.append([
                    comment['publishedAt'],
                    comment['textOriginal']
                ])
                if len(comments) >= 20:
                    break
    except googleapiclient.errors.HttpError as e:
        error_details = e.content.decode()
        if 'commentsDisabled' in error_details:
            print(f"Comments are disabled for video: {video}")
            return pd.DataFrame(columns=['updated_at', 'text'])

    df2 = pd.DataFrame(comments[:20], columns=['updated_at', 'text'])
    return df2

def search_videos(product_name, location):
    request = youtube.search().list(
        q=f"{product_name}",
        part="id",
        type="video",
        location=location,
        locationRadius='100km',
        maxResults=5  # Fetch more videos to increase the chance of getting comments
    )

    response = request.execute()
    video_ids = [item['id']['videoId'] for item in response['items']]
    return video_ids

def get_comments_for_product(product_name):
    df = pd.DataFrame(columns=['updated_at', 'text', 'country'])

    for coords, country in zip(coordinates, countries):
        location = f"{coords[0]},{coords[1]}"
        video_ids = search_videos(product_name, location)
        
        for video_id in video_ids:
            df2 = getcomments(video_id)
            if not df2.empty:
                df2['country'] = country
                df = pd.concat([df, df2], ignore_index=True)
            if df[df['country'] == country].shape[0] >= 20:
                break  # Stop if we have enough comments for this country

    return df

def stream_data(product_name):
    res = get_comments_for_product(product_name)
    print("res.............", res)
    try:
        res = get_comments_for_product(product_name)
        print("res.............", res)
        for index, row in res.iterrows():
            data = {
                "updated_at": row['updated_at'],
                "text": row['text'],
                "country": row['country']
            }
            print("Sending data to Kafka:", data)
            yield (
                json.dumps({"index": index}),
                json.dumps(data)
            )
        print("Les données sont envoyées")
    except Exception as e:
        logging.error(f'An error occurred: {e}')


