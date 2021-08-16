import spotipy
import spotipy.util as util
from functions import get_all_api_results, get_tracks_df, \
    get_artists_df, get_audio_feature_df, get_all_playlist_tracks_df, music_recommendations_df

import pandas as pd
#from spotipy.oauth2 import SpotifyOAuth

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# client credentials
id = 'xxxxxxxxxx'
secret = 'xxxxxxxxxx'
uri = 'xxxxxxxxxx'
user_name = 'xxxxxxxxxx'

# using scopes
scope = "user-library-read user-follow-read user-top-read playlist-read-private"

# Authorization flow
token = util.prompt_for_user_token(user_name, scope, client_id=id, client_secret=secret, redirect_uri=uri)

if token:
    sp = spotipy.Spotify(auth=token)
else:
    print("Can't get token for", user_name)

print("Extracting and transforming the top track data")
top_tracks = get_all_api_results(sp, sp.current_user_top_tracks())
top_tracks_df = get_tracks_df(top_tracks)
top_tracks_df = get_audio_feature_df(sp, top_tracks_df)
top_tracks_df.to_csv("/Users/Lawrence/Desktop/spotify_data/top_tracks.csv", index=False)


print("Extracting and transforming the top artists data")
top_artists = get_all_api_results(sp, sp.current_user_top_artists())
top_artists_df = get_artists_df(top_artists)
top_artists_df.to_csv("/Users/Lawrence/Desktop/spotify_data/top_artists.csv", index=False)


print("Extracting and transforming the followed artists data")
followed_artists = get_all_api_results(sp, sp.current_user_followed_artists())
followed_artists_df = get_artists_df(followed_artists)
followed_artists_df.to_csv("/Users/Lawrence/Desktop/spotify_data/followed_artists.csv", index=False)


print("Extracting and transforming the followed playlist")
followed_playlists_tracks = get_all_api_results(sp, sp.current_user_playlists())
playlists_df = pd.DataFrame(followed_playlists_tracks)
playlist_basic_df = playlists_df[['id','name']].drop_duplicates(ignore_index=True)
playlist_basic_df.to_csv("/Users/Lawrence/Desktop/spotify_data/playlists.csv", index=False)
# playlists_ids = playlists_df['id'].tolist()   # id list of all playlists
# playlists_names = playlists_df['name'].tolist()

print("Extracting and transforming the selected playlist track data")
selected_playlist = playlists_df[playlists_df['name'].str.contains('Boxing')]['id']
print(selected_playlist)
selected_playlist_tracks_df = get_all_playlist_tracks_df(sp, selected_playlist)
selected_playlist_tracks_df = get_audio_feature_df(sp, selected_playlist_tracks_df)
selected_playlist_tracks_df.to_csv("/Users/Lawrence/Desktop/spotify_data/followed_playlists_tracks.csv", index=False)

print("Extracting and transforming the recommended songs")
track_rec = music_recommendations_df(sp, selected_playlist_tracks_df, 20)
rec_tracks_df = get_tracks_df(track_rec).drop_duplicates(ignore_index=True)   # remove duplication
rec_tracks_df = get_audio_feature_df(sp, rec_tracks_df)
rec_tracks_df.to_csv("/Users/Lawrence/Desktop/spotify_data/recommended_tracks.csv", index=False)