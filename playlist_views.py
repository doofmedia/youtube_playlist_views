# -*- coding: utf-8 -*-

import os
import pprint
import csv
import pickle
import google.oauth2.credentials
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

CLIENT_SECRETS_FILE = 'client_secret.json'

# Login to a specific Google API service.
def get_service(API_SERVICE_NAME, API_VERSION, SCOPES):
    creds = None
    if os.path.exists('token-' + API_SERVICE_NAME + '.pickle'):
        with open('token-' + API_SERVICE_NAME + '.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CLIENT_SECRETS_FILE, SCOPES)
            creds = flow.run_console()
        # Save the credentials for the next run
        with open('token-' + API_SERVICE_NAME + '.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return build(API_SERVICE_NAME, API_VERSION, credentials=creds)

# Execute a YT Analytics request
def execute_api_request(client_library_function, **kwargs):
  response = client_library_function(
    **kwargs
  ).execute()

  return response['rows']

# Divide an array into chunks 
def chunks(l, n):
    n = max(1, n)
    return [l[i:i+n] for i in range(0, len(l), n)]

##################################
# THIS IS THE MAIN CODE THAT RUNS
##################################
if __name__ == '__main__':
  
  # Get the dates from the user.
  start_date = input('Start date (YYYY-MM-DD format): ')
  end_date = input('End date (YYYY-MM-DD format): ')

  # Disable OAuthlib's HTTPs verification when running locally.
  # *DO NOT* leave this option enabled when running in production.
  os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

  # Login to YT Analytics service
  youtubeAnalytics = get_service('youtubeAnalytics', 'v2', ['https://www.googleapis.com/auth/yt-analytics.readonly'])

  # Pull view count for top 200 videos (maximum allowed for now).
  # TODO: Switch to using video IDs and pulling the info for every video on the channel.
  view_results = execute_api_request(
      youtubeAnalytics.reports().query,
      ids='channel==MINE',
      startDate=start_date,
      endDate=end_date,
      metrics='views,averageViewPercentage',
      sort='-views',
      maxResults=200, # max is 200. 
      dimensions='video'
  )
  
  # Login to main YT service
  youtube = get_service('youtube', 'v3', ['https://www.googleapis.com/auth/youtube.readonly'])

  # Reduce all the ids of the videos into comma separated lists, then fetch their details.
  ids = [i[0] for i in view_results]
  chunked_ids = chunks(ids, 50)
  id_strings = [",".join(x) for x in chunked_ids]

  video_details = []
  for id_string in id_strings:
      request = youtube.videos().list(
            part="snippet,contentDetails",
            id=id_string # max is 50
        ).execute()
      video_details += request["items"]


  # Combine video info (title) with analytics data
  video_data = {}
  for result in view_results:
      video_data[result[0]] = {
          "title": next(x["snippet"]["title"] for x in video_details if x["id"] == result[0]),
          "views": result[1],
          "averageViewPercentage" : result[2],
          "adjustedViews" : result[1]*result[2]/100
      }

  # output a copy of the individual stats to a csv file.
  with open('individual_video_data.csv', mode='w') as csv_file:
    fieldnames = ['title', 'views', 'averageViewPercentage', 'adjustedViews']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    for video_datum in video_data:
        writer.writerow(video_data[video_datum])

  # Get all the playlists in our channel
  playlists = []
  nextPageToken = None
  while True:
      request = youtube.playlists().list(
            part="snippet,contentDetails",
            mine=True,
            maxResults=50,
            pageToken=nextPageToken
      )
      response = request.execute()
      playlists += [x for x in response["items"]]
      if "nextPageToken" in response:
          nextPageToken = response["nextPageToken"]
      else:
          break;

  # Get all the videos in each playlist, add them to the data structure
  # (since for whatever reason YT doesn't)
  for playlist in playlists:
      nextPageToken = None
      playlist["videos"] = []
      while True:
          request = youtube.playlistItems().list(
                part="contentDetails",
                maxResults=50,
                playlistId=playlist["id"],
                pageToken=nextPageToken
          )
          response = request.execute()
          playlist["videoCount"] = response["pageInfo"]
          playlist["videos"] += [x["contentDetails"]["videoId"] for x in response["items"]]
          if "nextPageToken" in response:
              nextPageToken = response["nextPageToken"]
          else:
              break;
  
  # Now, gather views by playlist
  playlists_data = []
  for playlist in playlists:
    playlist_adjusted_views = 0
    for video in playlist["videos"]:
        if video in video_data:
            playlist_adjusted_views += video_data[video]["adjustedViews"]
            del video_data[video]
    playlist_data = {
        "title": playlist["snippet"]["title"],
        "adjustedViews" : playlist_adjusted_views
    }

    playlists_data.append(playlist_data)

  # Finally, output grouped views to a CSV, with non-playlist videos listed separately underneath.
  with open('playlist_video_data.csv', mode='w') as csv_file:
    fieldnames = ['title', 'adjustedViews']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    for playlist_data in playlists_data:
      writer.writerow(playlist_data)
    writer.writerow({}) # insert a blank row to separate playlists from raw videos.
    for video_datum in video_data:
      writer.writerow(video_data[video_datum])