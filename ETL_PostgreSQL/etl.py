import os
import glob
import psycopg2
import pandas as pd
import sql_queries as sql


def process_song_file(cur, filepath):
    '''
    Opens the provided .json datafiles from the song folder, wrangles the provided data from the files
    and inserts them into the tables song_table and artist_table
    '''
    # open song file
    df = pd.read_json(filepath,lines = True)
    
    song_data = df[["song_id","title","artist_id","year","duration"]].values.tolist()
    cur.execute(sql.song_table_insert, song_data[0][:])
    
    # insert artist record
    artist_data = df[["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]].values.tolist()[0][:]
    cur.execute(sql.artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Opens the provided .json datafiles from the log folder, wrangles the provided data 
    and inserts them to the tables time_table, user_table and songplay
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == "NextSong"]

    # convert timestamp column to datetime
    t =  pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data_temp = pd.Series(t)
    time_data = [t, time_data_temp.dt.hour,time_data_temp.dt.day, time_data_temp.dt.isocalendar().week,
            time_data_temp.dt.month,time_data_temp.dt.year,time_data_temp.dt.weekday]
    column_labels = ('time', 'hour', 'day', 'week', 'month', 'year','weekday' )
    time_df = pd.concat(time_data,axis=1)
    time_df.columns = column_labels

    for i, row in time_df.iterrows():
        cur.execute(sql.time_table_insert, list(row))
        
    cur.execute("SELECT * FROM time_table")
    data = cur.fetchall()
    for row in data:
        print('timestamp', row[0])

    # load user table
    user_df = pd.concat([df['userId'],df['firstName'],df['lastName'],df['gender'],df['level']], axis =1)

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(sql.user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(sql.song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results[:2]
        else:
            songid, artistid = None, None
        # insert songplay record
        songplay_data = (time_df['time'][index], int(user_df['userId'][index]), user_df['level'][index], songid, artistid, 
                        int(df['sessionId'][index]), df['location'][index], df['userAgent'][index])
        
        cur.execute(sql.songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Uses the functions process_song_file and process_log_file to move data into the sparikify database
    '''
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
    '''
    Main entry point of the programm. Connects to the local sparkify database after it has been created in create_tables.py.
    runs the functions process_data, which uses the functions process_song_file and process_log_file to insert data into
    the database
    '''
    conn = psycopg2.connect(database="sparkifydb", user='postgres', password='LeonSiegner', host='127.0.0.1', port= '5432')
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()