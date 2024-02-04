# Author: Zongyu Wu
# Date: Feb. 4, 2024

library(RSQLite)
dbcon <- dbConnect(RSQLite::SQLite(), "lessonDB-WuZ.sqlitedb.")
dbExecute(dbcon, "PRAGMA foreign_keys = ON")

# List of tables
tables <- c("Lessons", "PreRequisites", "Modules", "Difficulties", "LessonModule")

# Drop each table
for (table in tables) {
  dropTable <- paste0("DROP TABLE IF EXISTS ", table)
  dbExecute(dbcon, dropTable)
}

# Create Lessons table
createLessons <- "CREATE TABLE if not exists Lessons(
    category TEXT,
    number INTERGER,
    title TEXT NOT NULL,
    pid INTERGER,
    lessonModuleId INTERGER not null,
    PRIMARY KEY (category, number),
    foreign key (pid) references PreRequisites(pid),
    foreign key (lessonModuleId) references LessonModule(lmid)
  );"
dbExecute(dbcon, createLessons)

# Create PreRequisites table
createPreRequisites <- "create table if not exists PreRequisites(
  pid NUMBER primary key,
  courseCat TEXT not null,
  courseNum INTERGER not null,
  preCat TEXT not null,
  preNum INTERGER not null,
  foreign key (courseCat, courseNum) references Lessons(category, number),
  foreign key (preCat, preNum) references Lessons(category, number)
);"
dbExecute(dbcon, createPreRequisites)

# Create Modules table
createModules <- "create table if not exists Modules(
  mid TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  lengthInMinutes NUMBER NOT NULL,
  difficulty NUMBER NOT NULL default 1,
  lessonModuleId INTERGER not null,
  foreign key (difficulty) references Difficulties (did),
  foreign key (lessonModuleId) references LessonModule(lmid)
);"
dbExecute(dbcon, createModules)

# Create look up table Difficulties for Modules
createDifficulties <- "create table if not exists Difficulties(
  did INTERGER primary key,
  difficulty TEXT not null
);"
dbExecute(dbcon, createDifficulties)

# Add difficulties to look up table
addDifficulties <- "insert into difficulties values
  (1, \"beginner\"),
  (2, \"intermediate\"),
  (3, \"advanced\"
);"
dbExecute(dbcon, addDifficulties)

# Create junction table LessonModule
createLessonModule <- "create table if not exists LessonModule(
  lmid INTERGER primary key,
  lCat TEXT not null,
  lNum INTERGER not null,
  mid TEXT not null,
  foreign key (lCat, lNum) references Lessons(category, number),
  foreign key (mid) references Modules (mid)
);"
dbExecute(dbcon, createLessonModule)


# Check structure of each table
for (table in tables) {
  checkStructure <- paste0("PRAGMA table_info(", table, ");")
  print(table)
  print(dbGetQuery(dbcon, checkStructure))
}


dbDisconnect(dbcon)