# functions that are going to be used to get the music data
import pandas as pd


def get_all_api_results(sp,sp_call):
    """
    Get all the results of tracks/artists from the api call
    :param sp: Spotify Oauth
    :param sp_call: Spotify_Api_Call
    :return: list of tracks/artists
    """
    results = sp_call

    if 'items' not in results.keys():
        results = results['artists']    # search for artists key if items is not in the JSON
    data = results['items']   # the tracks and artists information is in the items key; this has to be out of the if...else loop because it won't work for current user followed artists otherwise
    while results['next']:    # thers is limitation for the spotify api call return. so we need to search for 'next' part of data
        results = sp.next(results)
        if 'items' not in results.keys():
            results = results['artists']
        else:
            data.extend(results['items'])

    return data


def get_tracks_df(tracks):
    """
    transform the tracks data to df
    :param tracks: API results of tracks
    :return: tracks df
    """
    if len(tracks) == 0:
        pass
    else:
        tracks_df = pd.DataFrame(tracks)
        tracks_df['album_id'] = tracks_df['album'].apply(lambda x: x['id'])
        tracks_df['album_name'] = tracks_df['album'].apply(lambda x: x['name'])
        tracks_df['artist_id'] = tracks_df['artists'].apply(lambda x: x[0]['id'])
        tracks_df['artist_name'] = tracks_df['artists'].apply(lambda x: x[0]['name'])
        tracks_df['album_type'] = tracks_df['album'].apply(lambda x: x['album_type'])
        tracks_df['album_release_date'] = tracks_df['album'].apply(lambda x: x['release_date'])
        tracks_df['album_artist_id'] = tracks_df['album'].apply(lambda x: x['artists'][0]['id'])
        tracks_df['album_artist_name'] = tracks_df['album'].apply(lambda x: x['artists'][0]['name'])

        final_cols = ['id', 'name', 'popularity', 'type', 'is_local', 'explicit', 'duration_ms', 'disc_number',
                      'track_number', 'artist_id', 'artist_name', 'album_artist_id', 'album_artist_name',
                      'album_id', 'album_name', 'album_release_date', 'album_type']

        if 'added_at' in tracks_df.columns.tolist():
            final_cols.append('added_at')

    return tracks_df[final_cols]


def get_artists_df(artists):
    """
    this is used to transform the artitsts data to df
    :param artists: API results of artist
    :return: artists df
    """
    artists_df = pd.DataFrame(artists)
    artists_df['followers'] = artists_df['followers'].apply(lambda x: x['total'])   # write code for followers
    final_cols = ['id', 'name', 'popularity', 'followers', 'genres']

    return artists_df[final_cols]


def get_audio_feature_df(sp, tracks_df):
    """
    this is used to extract the audio feature data of tracks
    :param sp: Spotify OAuth
    :param tracks_df: tracks dataframe
    :return: dataframe of tracks df joining audio feature of tracks
    """
    track_ids = tracks_df['id'].to_list()
    audio_data = []

    for i in track_ids:
        audio_features = sp.audio_features(i)
        audio_df = pd.DataFrame(audio_features)
        audio_data.append(audio_df)

    audio_data = pd.concat(audio_data, ignore_index=True)

    tracks_df = tracks_df.merge(audio_data, on='id', how='left')
    tracks_df['genres'] = tracks_df['artist_id'].apply(lambda x: sp.artist(x)['genres'])
    tracks_df['album_genres'] = tracks_df['album_artist_id'].apply(lambda x: sp.artist(x)['genres'])

    return tracks_df


def get_all_playlists(playlists):
    """
    :param sp: Spotify OAuth
    :param playlist: playlist results from API call
    :return: lists of playlist ids and playlist names
    """
    user_playlists = get_all_api_results(playlists)
    playlists_df = pd.DataFrame(user_playlists)
    playlists_ids = playlists_df['id'].tolist()   # id list of all playlists
    playlists_names = playlists_df['name'].tolist()

    return playlists_ids, playlists_names

def get_all_playlist_tracks_df(sp, selected_playlists):
    """
    :param selected_playlists: playlists matching the keywords
    :param sp: Spotify OAuth
    :return: followed playlist df
    """
    appended_data = []

    for items in selected_playlists:

        playlist_tracks = sp.playlist(items, fields='tracks,next,name')
        tracks = playlist_tracks['tracks']
        data = tracks['items']   # details of all tracks in the playlist

        while tracks['next']:
            if len(tracks['next']) == 0:
                pass
            else:
                tracks = sp.next(tracks)
            data.extend(tracks['items'])   # data is a list containing all the tracks in the playlist. each item in the list is a dict

        playlist_tracks_df = pd.DataFrame(data)
        track_data = playlist_tracks_df['track']
        final_cols = ['id', 'name', 'popularity', 'type', 'is_local', 'explicit', 'duration_ms', 'disc_number',
                      'track_number', 'artist_id', 'artist_name', 'album_artist_id', 'album_artist_name',
                      'album_id', 'album_name', 'album_release_date', 'album_type']

        # construct a empty df
        df = pd.DataFrame(columns=final_cols)

        # write all the track features into the df columns
        for i in track_data.index:
            if track_data[i] == None:   # skip track data that is None type
                continue
            elif track_data[i]['is_local']:   # skip the local tracks that have no any feature value
                continue
            for c in range(0, len(final_cols)):
                if c <= 8:
                    df.loc[i, final_cols[c]] = track_data[i][final_cols[c]]
                df.loc[i, final_cols[9]] = track_data[i]['artists'][0]['id']
                df.loc[i, final_cols[10]] = track_data[i]['artists'][0]['name']
                df.loc[i, final_cols[11]] = track_data[i]['album']['artists'][0]['id']
                df.loc[i, final_cols[12]] = track_data[i]['album']['artists'][0]['name']
                df.loc[i, final_cols[13]] = track_data[i]['album']['id']
                df.loc[i, final_cols[14]] = track_data[i]['album']['name']
                df.loc[i, final_cols[15]] = track_data[i]['album']['release_date']
                df.loc[i, final_cols[16]] = track_data[i]['album']['type']

        appended_data.append(df)

    appended_data = pd.concat(appended_data, ignore_index=True)

    return appended_data


def music_recommendations_df(sp, track_df, rec_limit):
    """

    :param sp: Spotify OAuth
    :param track_df: target track df to get recommendations
    :param rec_limit: max number of recommendations
    :return: track list
    """
    track_list = track_df['id'].to_list()
    steps = []
    for i in range(0, len(track_list)+1, 5):   # decompose the tracks to packages (5 songs/package) because of the limit of the sp recommendation function
        steps.append(i)

    track_rec = []
    for s in steps:
        idx = steps.index(s)
        if idx == 0:
            continue
        for track_id in track_list[steps[idx-1]:steps[idx]]:
            track_recommendations = sp.recommendations(seed_tracks=[track_id], limit=rec_limit)
            track_rec.extend(track_recommendations['tracks'])

    return track_rec

