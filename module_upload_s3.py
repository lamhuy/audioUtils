import os
import re
import sys
import json
import boto3
import botocore
    
def upload_s3(ALBUM_NAME,ALBUM_KEY, ALBUM_DATE, ALBUM_SEARCH,ALBUM_SRC, ALBUM_LOC, ARTIST_NAME, ARTIST_KEY, ARTIST_SEARCH, BUCKET_NAME, DRYRUN):
          
    TRACKS_FILE_NAME = ALBUM_NAME + '.json'
    PATH_MP3 = ARTIST_NAME + ' - '  + ALBUM_NAME
    
    BUCKET_PATH = ARTIST_KEY + '/' + ALBUM_KEY + '/'
    S3_BUCKET = 'http://'+BUCKET_NAME+'.s3-website-us-east-1.amazonaws.com/'
    s3 = boto3.resource('s3')
    
    #if found no tracks.json will terminaet
    with open(TRACKS_FILE_NAME,  encoding='utf8') as data_file:          
        tracks_titles = json.load(data_file)
        print(json.dumps(tracks_titles, ensure_ascii=False).encode('utf8'))
        
 
    playlist_json = {}
    playlist_json["title"] = ALBUM_NAME
    playlist_json["playlist"] = []
    for i, track in enumerate(tracks_titles):
      #  track = track.decode('utf-8')
        if(i == len(tracks_titles)-1):
            break;
            
        #print(i)
        #print(len(tracks_titles))
        playlist_track = {}
        playlist_track["title"] = track
        if ARTIST_NAME:
            playlist_track["artist"] = ARTIST_NAME
        playlist_track["src"] = S3_BUCKET + BUCKET_PATH + track + '.mp3'
        
        playlist_json["playlist"].append(playlist_track)
    #print(playlist_json) 
    
    print('Uploading mp3 tracks')
    dirPath = os.path.dirname(os.path.realpath(__file__))  # /home/user/test
    
    for i, track in enumerate(tracks_titles):
        if(i == len(tracks_titles)-1):
            break;
        mp3Filename = dirPath + '/' + PATH_MP3+ '/' + track+'.mp3'
        if not DRYRUN:
            s3.Object(BUCKET_NAME, BUCKET_PATH + track+'.mp3').put(Body=open(mp3Filename, 'rb'))
    
    
    
    #upload playlist into aws    
    PLAYLIST_FILE = 'playlist_' + ALBUM_KEY + '.json'  
    print('Uploading playlist:' + PLAYLIST_FILE)
    
    # Uploads the given file using a managed uploader, which will split up large
    # files automatically and upload parts in parallel.
    #print(playlist_json)
    print(json.dumps(playlist_json, ensure_ascii=False).encode('utf8'))
    if not DRYRUN:
        s3.Object(BUCKET_NAME, BUCKET_PATH +PLAYLIST_FILE).put(Body=json.dumps(playlist_json, ensure_ascii=False).encode('utf8'))
    
    
    #download dhramaCast_ARTIST_KEY.json, append this dharmaCast_ARTIST_KEY.json info, then upload
    ARTIST_JSON_FILE = ARTIST_KEY + '/dharmaCast_'+ARTIST_KEY +'.json'

    #adding album into into artist dharma cast list
    
    #try to find artist json file, that list all artist album
    try:
        artist_json = s3.Object(BUCKET_NAME, ARTIST_JSON_FILE).get()["Body"]
        artist_json = json.load(artist_json)
    except botocore.exceptions.ClientError as e:
        print(e.response['Error']['Code'])
        if e.response['Error']['Code'] == "NoSuchKey":
            print("The object does not exist. creating ARTIST_JSON_FILE")
            artist_json = {}
            artist_json['title'] = ARTIST_NAME
            artist_json['key'] = ARTIST_KEY           
            artist_json['playlists'] = []
           
        else:
            raise
    
    print('Artist play list before')    
#    print(artist_json)    
    print(json.dumps(artist_json["playlists"], ensure_ascii=False).encode('utf8'))
    #print(artist_json["playlists"])
    
    albumExist = False

    for i, playlist in enumerate(artist_json["playlists"]):        
        if(ALBUM_KEY == playlist["listName"]):
            albumExist = True
            break;


    if(not albumExist):
        print("adding album to artist_json")
        #build json object, append it
        data = {}
        data['title'] = ALBUM_NAME
        data['listName'] = ALBUM_KEY
        if ALBUM_DATE:
            data['date'] = ALBUM_DATE
        if ALBUM_SEARCH:
            data['search'] = ALBUM_SEARCH
        if ALBUM_SRC:
            data['src'] = ALBUM_SRC
        if ALBUM_LOC:
            data['loc'] = ALBUM_LOC
        else:
            data['search'] = ALBUM_NAME + ' ' + ALBUM_KEY
        artist_json["playlists"].append(data)
        print(json.dumps(artist_json["playlists"], ensure_ascii=False).encode('utf8'))
        #print(artist_json["playlists"])
        #upload new file into S3
        if not DRYRUN:
            s3.Object(BUCKET_NAME, ARTIST_JSON_FILE).put(Body=json.dumps(artist_json, ensure_ascii=False).encode('utf8'))

    else:
        print("ALBUM EXIST")


    #adding artist into dharma cast.json list
    DHARMA_JSON_FILE = 'dharmaCast.json'
    dharma_json = s3.Object(BUCKET_NAME, DHARMA_JSON_FILE).get()["Body"]
    artistExist = False

    dharma_json = json.load(dharma_json)
    print("Current Dharma.json file content: ")
    print(json.dumps(dharma_json, ensure_ascii=False).encode('utf8'))
    #print(dharma_json)

    for i, artist in enumerate(dharma_json):        
        if(ARTIST_KEY == artist["listName"]):
            artistExist = True
            break;


    if(not artistExist):
        print("adding artist to dharma_json")
        #build json object, append it
        data = {}
        data['title'] = ARTIST_NAME
        data['listName'] = ARTIST_KEY
        if ARTIST_SEARCH:
            data['search'] = ARTIST_SEARCH
        else:
            data['search'] = ARTIST_NAME + ' ' + ARTIST_KEY
        data['id'] = len(dharma_json)+1
        dharma_json.append(data)
        #print(dharma_json)
        print(json.dumps(dharma_json, ensure_ascii=False).encode('utf8'))
        #upload new file into S3
        if not DRYRUN:
            s3.Object(BUCKET_NAME, DHARMA_JSON_FILE).put(Body= json.dumps(dharma_json, ensure_ascii=False).encode('utf8'))

    else:
        print("ARTIST EXIST")
    