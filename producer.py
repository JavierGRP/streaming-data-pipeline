import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: x.encode('utf-8'))

print("Starting Spotify producer")

client_id = "Your client ID"
client_secret = "Your client secret ID"

client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)

def getTrackID(user, playlist_id):
        id = []
        playlist = sp.user_playlist(user, playlist_id)
        for item in playlist['tracks']['items']:
                track = item['track']
                id.append(track['id'])
        return id

def getTrackFeatures(id):
        meta = sp.track(id)
        features = sp.audio_features(id)
		    # meta data
        name = meta['name']
        album = meta['album']['name']
        artist = meta['album']['artists'][0]['name']

        # features from data
        danceability = features[0]['danceability']

        track = [name, album, artist, danceability]
        return track

tracks = []
while True:
        ids = getTrackID('javiergrp', '15XvyZ84e5tXUVoWd15Eqm')
        for id in range(len(ids)):
                track = getTrackFeatures(ids[id])
                if track not in tracks:
                        tracks.append(track)
                        print(track)
                        msg = track[0] + ',' + track[1] + ',' + track[2] + ',' + str(track[3])
                        producer.send('spotifyTopic', msg)
