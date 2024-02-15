# name: ASSIGNMENT 06.1: Build Triggers in SQLite 
# date: 2/13/2024
# author: Zongyu Wu

library(RSQLite)
dbcon <- dbConnect(RSQLite::SQLite(), "MediaDB.db")


# Tast1
# Add column if not exists
checkStructure <- "PRAGMA table_info(albums);"
existingColumns <- dbGetQuery(dbcon, checkStructure)
if (!"play_time" %in% existingColumns$name) {
  addColumn <- "alter table albums add column play_time REAL;"
  dbExecute(dbcon, addColumn)
}
print(dbGetQuery(dbcon, checkStructure))


# Task2
# Create copy table
dbExecute(dbcon, "drop table if exists albums_copy")
createCopy <- "
CREATE TABLE albums_copy (
    AlbumId INTEGER PRIMARY KEY AUTOINCREMENT,
    Title NVARCHAR(160) not null,
    ArtistID INTEGER not null,
    play_time REAL
    CHECK (play_time > 0)
)"
dbExecute(dbcon, createCopy)

# Copy data
copyData <- "
INSERT INTO albums_copy (AlbumId, Title, ArtistID, play_time)
SELECT AlbumId, Title, ArtistID, play_time FROM albums
"
dbExecute(dbcon, copyData)
print(dbGetQuery(dbcon, checkStructure))
print(dbGetQuery(dbcon, "select * from albums_copy limit 5"))


# Task3
updatePlayTime <- "update albums_copy
  set play_time = (
  select sum(Milliseconds) / 60000.0
  from tracks
  where tracks.AlbumID = albums_copy.AlbumID
);"
dbExecute(dbcon, updatePlayTime)
print(dbGetQuery(dbcon, "select * from albums_copy limit 5"))


# Task4
dbExecute(dbcon, "drop trigger if exists insert_play_time;")
insertTrigger <- "create trigger if not exists insert_play_time after insert on tracks
begin
  update albums_copy
  set play_time = (
    select sum(Milliseconds) / 60000.0 
    from tracks
    where AlbumID = NEW.AlbumID
  )
  where AlbumID = NEW.AlbumID;
end;"
dbExecute(dbcon, insertTrigger)


# Task5
dbExecute(dbcon, "drop trigger if exists update_play_time;")
updateTrigger <- "create trigger if not exists update_play_time after update on tracks
begin
  update albums_copy
  set play_time = (
    select sum(Milliseconds) / 60000.0 
    from tracks
    where AlbumID = OLD.AlbumID
  )
  where AlbumID = OLD.AlbumID;
end;"
dbExecute(dbcon, updateTrigger)

dbExecute(dbcon, "drop trigger if exists delete_play_time;")
deleteTrigger <- "create trigger if not exists delete_play_time after delete on tracks
begin
  update albums_copy
  set play_time = (
    select sum(Milliseconds) / 60000.0 
    from tracks
    where AlbumID = OLD.AlbumID
  )
  where AlbumID = OLD.AlbumID;
end;"
dbExecute(dbcon, deleteTrigger)


# Task6
# Check play time of the first album
print(dbGetQuery(dbcon, "select * from albums_copy where AlbumId = 1"))
dbExecute(dbcon, "delete from tracks
          where TrackId = 10000;")

# Insert into track
dbExecute(dbcon, "insert into tracks (TrackId, Name, AlbumId, MediaTypeId, GenreId, Composer, Milliseconds, Bytes, UnitPrice)
          values(10000, 'name', 1, 1, 1, 'me', 60000, 1, 1);")
print(dbGetQuery(dbcon, "select * from albums_copy where AlbumId = 1"))

# Update
dbExecute(dbcon, "update tracks 
          set Milliseconds = 120000
          where TrackId = 10000;")
print(dbGetQuery(dbcon, "select * from albums_copy where AlbumId = 1"))

# Delete
dbExecute(dbcon, "delete from tracks
          where TrackId = 10000;")
print(dbGetQuery(dbcon, "select * from albums_copy where AlbumId = 1"))

dbDisconnect(dbcon)

# We can see that play_time of AlbumId = 1 changes after insert, update, and delete operation.