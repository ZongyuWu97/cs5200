# Librarys
library("RMariaDB")
library("DBI")

# Settings
db_user <- 'admin'            # use your value from the setup
db_password <- 'wuzongyu'    # use your value from the setup
db_name <- 'practicum1'         # use your value from the setup

db_host <- 'cs5200.ceko8sdfannu.us-east-1.rds.amazonaws.com'       # for aws

db_port <- 3306

# Connect to remote server database

mydb.aws <-  DBI::dbConnect(RMariaDB::MariaDB(), user = db_user, password = db_password,
                            dbname = db_name, host = db_host, port = db_port)


# Helper functions

findAirportPK <- function (conn, aName, aCode, aState)
{
  # aid is the return value
  aid <- 0
  
  # does airport already exist?
  query <- "select aid from airports where airportName = ?"
  res <- dbGetQuery(conn, query, params = list(aName))
  
  if (nrow(res) > 0) {
    aid <- res$aid
  }
  return (aid)
}


createAirportPK <- function (conn, aName, aCode, aState)
{
  # aid is the return value
  aid <- 0
  
  query <- "select MAX(aid) aid from airports where airportName = ?"
  res <- dbGetQuery(conn, query, params = list(aName))
  
  if (is.na(res$aid)) {
    query <- "select MAX(aid) aid from airports"
    res <- dbGetQuery(conn, query)
    if (is.na(res$aid)) {
      res$aid <- 0
    }
    # return value: new aid is new row number
    aid <- res$aid + 1
  }
  
  return (aid)
}


findConditionPK <- function (conn, sky_condition, explanation)
{
  # aid is the return value
  cid <- 0
  
  # does airport already exist?
  query <- "select cid from conditions where sky_condition = ?"
  res <- dbGetQuery(conn, query, params = list(sky_condition))
  
  if (nrow(res) > 0) {
    cid <- res$cid
  }
  
  return (cid)
}


createConditionPK <- function (conn, sky_condition, explanation)
{
  # aid is the return value
  cid <- 0
  
  query <- "select MAX(cid) cid from conditions where sky_condition = ?"
  res <- dbGetQuery(conn, query, params = list(sky_condition))
  
  if (is.na(res$cid)) {
    query <- "select MAX(cid) cid from conditions"
    res <- dbGetQuery(conn, query)
    
    # return value: new aid is new row number
    if (is.na(res$cid)) {
      res$cid <- 0
    }
    cid <- res$cid + 1
  }

  return (cid)
}


findFlightPK <- function (conn, fDate, fOriginAirport, fAirlineName, fAircraftType, fIsHeavy)
{
  # fid is the return value
  fid <- 0
  
  # does airport already exist?
  query <- "select fid from flights where date = ? and originAirport = ? and airlineName = ? and aircraftType = ? and isHeavy = ?"
  res <- dbGetQuery(conn, query, params = list(fDate, fOriginAirport, fAirlineName, fAircraftType, fIsHeavy))
  
  if (nrow(res) > 0) {
    fid <- res$fid
  }
  return (fid)
}


createFlightPK <- function (conn, fDate, fOriginAirport, fAirlineName, fAircraftType, fIsHeavy)
{
  # fid is the return value
  fid <- 0
  
  query <- "select MAX(fid) fid from flights where date = ? and originAirport = ? and airlineName = ? and aircraftType = ? and isHeavy = ?"
  res <- dbGetQuery(conn, query, params = list(fDate, fOriginAirport, fAirlineName, fAircraftType, fIsHeavy))

  if (is.na(res$fid)) {
    query <- "select MAX(fid) fid from flights"
    res <- dbGetQuery(conn, query)
    # return value: new aid is new row number
    if (is.na(res$fid)) {
      res$fid <- 0
    }
    fid <- res$fid + 1
  }
  return (fid)
}


getIDs <- function(conn, row)
{
  # Use other information to find airport PK
  aName <- row$airport
  aState <- row$origin
  aCode <- ''
  if (nchar(aName) == 0)
    aName <- "UNKNOWN"
  aid <- findAirportPK(conn, aName, aCode, aState)
  
  
  # Get condition PK
  sky_condition <- row$sky_conditions
  explanation <- ''
  if (nchar(sky_condition) == 0)
    sky_condition <- "UNKNOWN"
  cid <- findConditionPK(conn, sky_condition, explanation)
  
  
  # Get flight PK
  fDate <- row$flight_date
  fOriginAirport <- aid
  fAirlineName <- row$airline
  fAircraftType <- row$aircraft
  fIsHeavy <- row$heavy_flag
  if (nchar(fDate) == 0)
    fDate <- ""
  fDate <- substr(fDate, 1, nchar(fDate)-5)
  fDate <- as.Date(fDate, format = "%m/%d/%y")
  if (nchar(fAirlineName) == 0)
    fAirlineName <- "UNKNOWN"
  if (fIsHeavy == "Yes")
    fIsHeavy <- TRUE
  else
    fIsHeavy <- FALSE
  fid <- findFlightPK(conn, fDate, fOriginAirport, fAirlineName, fAircraftType, fIsHeavy)
  
  
  sid <- row$rid
  result <- list(fid = fid, aid = aid, cid = cid, sid = sid)
  
  return(result) 
}


clearCSV <- function(conn, csv)
{
  txnFailed = FALSE
  
  dbExecute(conn, "START TRANSACTION")
  n <- nrow(csv)
  for (r in 1:n)
  {
    ids <- getIDs(conn, csv[r, ])
    if (ids$sid != 0) {
      ps1 <- dbSendStatement(conn, "DELETE FROM strikes where sid = ?", params = list(ids$sid))
      # if (dbGetRowsAffected(ps1) < 1)
      #   txnFailed = TRUE
      dbClearResult(ps1)
    }
    
    if (ids$fid != 0) {
      ps2 <- dbSendStatement(conn, "delete from flights where fid = ?", params = list(ids$fid))
      # if (dbGetRowsAffected(ps2) < 1)
      #   txnFailed = TRUE
      dbClearResult(ps2)
    }
    if (ids$cid != 0) {
      ps3 <- dbSendStatement(conn, "delete from conditions where cid = ?", params = list(ids$cid))
      # if (dbGetRowsAffected(ps3) < 1)
      #   txnFailed = TRUE
      dbClearResult(ps3)
    }
    if (ids$aid != 0) {
      ps4 <- dbSendStatement(conn, "delete from airports where aid = ?", params = list(ids$aid))
      # if (dbGetRowsAffected(ps4) < 1)
      #   txnFailed = TRUE
      dbClearResult(ps4)
    }

    # commit transaction if no failure, otherwise rollback
    if (txnFailed == TRUE) {
      dbExecute(conn, "ROLLBACK")
      break
    }
  }
  
  if (txnFailed == FALSE) {
    dbExecute(conn, "COMMIT")
  }
  
  return (!txnFailed)
}


insertCSV <- function(conn, csvName, transaction = TRUE)
{
  csv <- read.csv(csvName, header = T, stringsAsFactors = F)
  clear <- clearCSV(conn, csv)
  if (clear) {
    print("Clear success")
  } else {
    print("Clear fail")
  }
  
  n <- nrow(csv)
  if (transaction == TRUE) { 
    dbExecute(conn, "START TRANSACTION")
    txnFailed = FALSE
  }
  
  for (r in 1:n)
  {
    ids <- getIDs(conn, csv[r, ])
    aid = ids$aid
    cid = ids$cid
    fid = ids$fid
    sid = ids$sid
    
    # airports
    aName <- csv[r, 'airport']
    aState <- csv[r, 'origin']
    aCode <- ''
    if (nchar(aName) == 0) {
      aName <- "UNKNOWN"
    }
    if (aid == 0) {
      aid <- createAirportPK (conn, aName, aCode, aState)
    
      if (aid != 0) {
        sql <- "insert into airports (aid, airportName, airportCode, airportState) values (?, ?, ?, ?)"
        ps <- dbSendStatement(conn, sql, immediate = F)
        dbBind(ps, params = list(aid, aName, aCode, aState))
        nr <- dbGetRowsAffected(ps)
        if (transaction == TRUE) { 
          if (nr < 1) {
            txnFailed = TRUE
          }
        }
        dbClearResult(ps)
        Sys.sleep(0.5)
      }
    }
    
    
    # conditions
    sky_condition <- csv[r, 'sky_conditions']
    if (nchar(sky_condition) == 0)
      sky_condition <- "UNKNOWN"
    explanation <- ''
    if (cid == 0) {
      cid <- createConditionPK(conn, sky_condition, explanation)
      if (cid != 0) {
        sql <- "insert into conditions (cid, sky_condition, explanation) values (?, ?, ?)"
        ps <- dbSendStatement(conn, sql, immediate = F)
        dbBind(ps, params = list(cid, sky_condition, explanation))
        nr <- dbGetRowsAffected(ps)
        if (transaction == TRUE) { 
          if (nr < 1)  {     
            txnFailed = TRUE
          }
        }
        dbClearResult(ps)
        Sys.sleep(0.5)
      }
    }
    
    
    # flights
    fDate <- csv[r,'flight_date']   
    fOriginAirport <- aid
    fAirlineName <- csv[r,'airline']
    fAircraftType <- csv[r,'aircraft']
    fIsHeavy <- csv[r,'heavy_flag']
    if (nchar(fDate) == 0)
      fDate <- ""
    fDate <- substr(fDate, 1, nchar(fDate)-5)
    fDate <- as.Date(fDate, format = "%m/%d/%y")
    if (nchar(fAirlineName) == 0)
      fAirlineName <- "UNKNOWN"
    if (fIsHeavy == "Yes")
      fIsHeavy <- TRUE
    else
      fIsHeavy <- FALSE
    
    if (fid == 0) {
      fid <- createFlightPK(conn, fDate, aid, fAirlineName, fAircraftType, fIsHeavy)
      if (fid != 0) {
        sql <- "insert into flights (fid, date, originAirport, airlineName, aircraftType, isHeavy) values (?, ?, ?, ?, ?, ?)"
        ps <- dbSendStatement(conn, sql, immediate = F)
        dbBind(ps, params = list(fid, fDate, fOriginAirport, fAirlineName, fAircraftType, fIsHeavy))
        nr <- dbGetRowsAffected(ps)
        if (transaction == TRUE) { 
          if (nr < 1)  {     
            txnFailed = TRUE
          }
        }
        dbClearResult(ps)
        Sys.sleep(0.5)
      }
    }
    
    
    # strikes
    sid <- csv[r, 'rid']
    numbirds <- 1
    impact <- csv[r, 'impact']
    altitude <- csv[r, 'altitude_ft']
    if (csv[r, 'damage'] == "Damage" | csv[r, 'damage'] == "Loss")
      damage <- TRUE
    else
      damage <- FALSE
    
    if (sid != 0) {
      sql <- "insert into strikes (sid, fid, numbirds, impact, damage, altitude, conditions) values (?, ?, ?, ?, ?, ?, ?)"
      ps <- dbSendStatement(conn, sql, immediate = F)
      dbBind(ps, params = list(sid, fid, numbirds, impact, damage, altitude, cid))
      nr <- dbGetRowsAffected(ps)
      if (transaction == TRUE) { 
        if (nr < 1)  {     
          txnFailed = TRUE
        }
      }
      dbClearResult(ps)
      Sys.sleep(0.5)
    }
    
    
    if (transaction == TRUE) { 
      if (txnFailed == TRUE) {
        dbExecute(conn, "ROLLBACK")
        break
      }
    }
  }
  
  if (transaction == TRUE) { 
    if (txnFailed == FALSE)
      dbExecute(conn, "COMMIT")
  }
  
  if (transaction == TRUE) { 
    if (!txnFailed) {
      return ("Transaction committed")
    } else {
      return("Transaction rollbacked")
    }
  } else {
    return ("Done, not a transaction")
  }
}




tryCatch(
  {
    # Check if the CSV filename is provided as an argument
    if (length(commandArgs(trailingOnly = TRUE)) == 0) {
      csvName <- 'new1.csv'
      transaction <- TRUE
    } else if (length(commandArgs(trailingOnly = TRUE)) > 2) {
      stop("Usage: Rscript week10.R <csvName> <transaction>")
    } else if (length(commandArgs(trailingOnly = TRUE)) == 1){
      csvName <- commandArgs(trailingOnly = TRUE)[1]
      transaction <- TRUE
    } else if ((length(commandArgs(trailingOnly = TRUE)) == 2)) {
      csvName <- commandArgs(trailingOnly = TRUE)[1]
      transaction <- commandArgs(trailingOnly = TRUE)[2]
    }
    
    res <- insertCSV(conn = mydb.aws, csvName, transaction)
    print(res)
  },
  finally = {
    dbDisconnect(mydb.aws)
  }
)



