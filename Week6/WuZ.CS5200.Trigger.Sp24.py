# name: ASSIGNMENT 06.1: Build Triggers in SQLite 
# date: 2/14/2024
# author: Zongyu Wu

import sqlite3

try:
  dbcon = sqlite3.connect('MediaDB.db')
  cursor = dbcon.cursor()
  print("connection successful")

  
  # Tast1
  print("\n\nTask1\n")
  # Add column if not exists
  checkStructure = "PRAGMA table_info(albums);"
  existingColumns = [x[1] for x in cursor.execute(checkStructure).fetchall()]
  print("Existing columns:\n", existingColumns) 
  if not ("play_time" in existingColumns):
    addColumn = "alter table albums add column play_time REAL;"
    cursor.execute(addColumn)
    existingColumns = [x[1] for x in cursor.execute(checkStructure).fetchall()]
    print("New columns:\n", existingColumns)  
  dbcon.commit()
  
  
  # Task2
  print("\n\nTask2\n")
  # Create copy table
  cursor.execute("drop table if exists albums_copy")
  createCopy = '''
  CREATE TABLE albums_copy (
      AlbumId INTEGER PRIMARY KEY AUTOINCREMENT,
      Title NVARCHAR(160) not null,
      ArtistID INTEGER not null,
      play_time REAL
      CHECK (play_time > 0)
  )'''
  cursor.execute(createCopy)
  
  # Copy data
  copyData = '''
  INSERT INTO albums_copy (AlbumId, Title, ArtistID, play_time)
  SELECT AlbumId, Title, ArtistID, play_time FROM albums
  '''
  cursor.execute(copyData)
  print("Columns of copy:\n", cursor.execute(checkStructure).fetchall())
  print("Copied data:\n", cursor.execute("select * from albums_copy limit 5").fetchall())
  
  
  # Task3
  print("\n\nTask3\n")
  updatePlayTime = '''update albums_copy
    set play_time = (
    select sum(Milliseconds) / 60000.0
    from tracks
    where tracks.AlbumID = albums_copy.AlbumID
  );'''
  cursor.execute(updatePlayTime)
  print("Calculate play time:\n", cursor.execute("select * from albums_copy limit 5").fetchall())
  
  
  # Task4
  print("\n\nTask4\n")
  cursor.execute("drop trigger if exists insert_play_time;")
  insertTrigger = '''create trigger if not exists insert_play_time after insert on tracks
  begin
    update albums_copy
    set play_time = (
      select sum(Milliseconds) / 60000.0 
      from tracks
      where AlbumID = NEW.AlbumID
    )
    where AlbumID = NEW.AlbumID;
  end;'''
  cursor.execute(insertTrigger)
  
  
  # Task5
  print("\n\nTask5\n")
  cursor.execute("drop trigger if exists update_play_time;")
  updateTrigger = '''create trigger if not exists update_play_time after update on tracks
  begin
    update albums_copy
    set play_time = (
      select sum(Milliseconds) / 60000.0 
      from tracks
      where AlbumID = OLD.AlbumID
    )
    where AlbumID = OLD.AlbumID;
  end;'''
  cursor.execute(updateTrigger)
  
  cursor.execute("drop trigger if exists delete_play_time;")
  deleteTrigger = '''create trigger if not exists delete_play_time after delete on tracks
  begin
    update albums_copy
    set play_time = (
      select sum(Milliseconds) / 60000.0 
      from tracks
      where AlbumID = OLD.AlbumID
    )
    where AlbumID = OLD.AlbumID;
  end;'''
  cursor.execute(deleteTrigger)
  
  
  # Task6
  print("\n\nTask6\n")
  # Check play time of the first album
  print("Before insert:\n", cursor.execute("select * from albums_copy where AlbumId = 1").fetchall())
  cursor.execute("delete from tracks where TrackId = 10000;")
  
  # Insert into track
  cursor.execute("insert into tracks (TrackId, Name, AlbumId, MediaTypeId, GenreId, Composer, Milliseconds, Bytes, UnitPrice) values(10000, 'name', 1, 1, 1, 'me', 60000, 1, 1);")
  print("After insert:\n", cursor.execute("select * from albums_copy where AlbumId = 1").fetchall())
  
  # Update
  cursor.execute('''update tracks 
            set Milliseconds = 120000
            where TrackId = 10000;''')
  print("After update:\n", cursor.execute("select * from albums_copy where AlbumId = 1").fetchall())
  
  # Delete
  cursor.execute("delete from tracks where TrackId = 10000;")
  print("After delete:\n", cursor.execute("select * from albums_copy where AlbumId = 1").fetchall())
  

  print("\nWe can see that play_time of AlbumId = 1 changes after insert, update, and delete operation.")
  cursor.close()

except sqlite3.Error as error:
  print("can't connect", error)

finally:
  if dbcon:
      dbcon.commit()
      dbcon.close()
      print("connection closed")
