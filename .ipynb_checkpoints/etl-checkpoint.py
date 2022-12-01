import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Function to process a given song file.
    
    Executes the queries to insert in the DB the song and the artist found in the song file.
    
    Parameters
    ----------
    cur : Object
        Cursor to PostgreSQL database.
    filepath : str
        Path to the file which has to be examinated. 
    
    """
    # open song file
    df = pd.read_json(filepath, lines=True).replace({np.nan: None})
    
    # insert song record
    temp = df[["song_id","title","artist_id","year","duration"]]
    temp.head()
    song_data = temp.values[0].tolist()

    cur.execute(song_table_insert, song_data)
    
    
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    """
    Function to process a given log file.
    
    Executes the queries to insert records in the time, users and songplay tables.
    
    Parameters
    ----------
    cur : Object
        Cursor to PostgreSQL database.
    filepath : str
        Path to the file which has to be examinated. 
    
    """
    # open log file
    df = pd.read_json(filepath, lines=True).replace({np.nan: None})

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit='ms')
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")

    # combining column_labels and time_data into a dictionary
    dic = {}
    for i in range(len(time_data)):
        dic[column_labels[i]] = time_data[i]
        
    time_df = pd.DataFrame(data = dic, columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, list(row))

    df["start_time"] = pd.to_datetime(df["ts"], unit='ms')

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplayid = str(row.sessionId) + str(row.itemInSession)
        
        # insert songplay record
        songplay_data = (songplayid, row.start_time, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """
    Function which coordinates the etl pipeline.
    
    Examines the folders looking for log and song data and makes the corresponding calls to the functions which read those files.
    
    Parameters
    ----------
    cur : Object
        Cursor to PostgreSQL database.
    conn: Object
        Connection to the PostgreSQL database
    filepath : str
        Path to the file which has to be examinated. 
    func: Object
        Function to be called. Either the one corresponding to the song or to the log files.
    
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Main function.
    
    Creates a connection to the database and calls the functions to process the data.
    
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()