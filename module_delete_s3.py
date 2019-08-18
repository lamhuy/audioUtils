import os
import re
import sys
import json
import boto3
import botocore

    
def delete_s3(ALBUM_KEY, ARTIST_KEY, BUCKET_NAME, DRYRUN):
          
    BUCKET_PATH = ARTIST_KEY + '/' + ALBUM_KEY + '/'
    #S3_BUCKET = 'http://'+BUCKET_NAME+'.s3-website-us-east-1.amazonaws.com/'
    
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(BUCKET_NAME)

    objects_to_delete = []
    for obj in bucket.objects.filter(Prefix=ARTIST_KEY + '/' + ALBUM_KEY + '/'):
        objects_to_delete.append({'Key': obj.key})

    bucket.delete_objects(
        Delete={
            'Objects': objects_to_delete
        }
    )
    
     
    #download dhramaCast_ARTIST_KEY.json, append this dharmaCast_ARTIST_KEY.json info, then upload
    ARTIST_JSON_FILE = ARTIST_KEY + '/dharmaCast_'+ARTIST_KEY +'.json'

    #remove album from artist dharma cast list
    
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
    
    albumIndex = 0
    albumExist = False
    for i, playlist in enumerate(artist_json["playlists"]): 
        if(ALBUM_KEY == playlist["listName"]):
            albumExist = True
            albumIndex = i
            break;


    if(albumExist):
        print("removing album from artist_json")
        artist_json["playlists"].pop(albumIndex)
        print(json.dumps(artist_json["playlists"], ensure_ascii=False).encode('utf8'))
        #upload new file into S3
        if not DRYRUN:
            s3.Object(BUCKET_NAME, ARTIST_JSON_FILE).put(Body=json.dumps(artist_json, ensure_ascii=False).encode('utf8'))

    else:
        print("ALBUM NOT EXIST????")
    