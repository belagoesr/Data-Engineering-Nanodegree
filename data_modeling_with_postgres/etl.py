import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    """
    Reads an song entry stored in filepath and insert data in the songs and artists tables
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert song record
    song_data = [df.iloc[0]['song_id'], df.iloc[0]['title'], df.iloc[0]['artist_id'], df.iloc[0]['year'].item(), df.iloc[0]['duration']]
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = [df.iloc[0]['artist_id'], df.iloc[0]['artist_name'], df.iloc[0]['artist_location'], df.iloc[0]['artist_latitude'], df.iloc[0]['artist_longitude']]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Reads  a event log file stored in filepath and insert data into the songplays table
    """
    # open log file
    df = pd.read_json(filepath, lines=True)
    
    # filter by NextSong action
    df = df[(df.page == 'NextSong')]
    
    
    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(columns=column_labels)
    time_df['start_time'] = t;
    time_df['hour'] = t.map(lambda x: x.hour)
    time_df['day'] = t.map(lambda x: x.day)
    time_df['week'] = t.map(lambda x: x.week)
    time_df['month'] = t.map(lambda x: x.month)
    time_df['year'] = t.map(lambda x: x.year)
    time_df['weekday'] = t.map(lambda x: x.weekday())
    
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    
    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
   
    # insert user records
    for i, row in user_df.iterrows():
        # cast user_id to int
        mapped_row = [int(row[0])] + list(row[1:])
        cur.execute(user_table_insert, mapped_row)
    
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        
        # insert songplay record
        songplay_data = [pd.to_datetime(row.ts, unit='ms'), int(row.userId), songid, artistid, row.level, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)
    
def process_data(cur, conn, filepath, func):
    """
    Navigates filepath, gather all files in under filepath and apply func to each file.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        # hidden directories of .ipynb_checkpoints was being accounted and causing issues
        # filter it out of file list
        files = list(filter(lambda x: x.find('.ipynb_checkpoints') == -1 , files))
        for f in files:
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()