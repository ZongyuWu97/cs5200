## Zongyu WU, 24SP CS5200, 4/13/2024


library(XML)
library(RSQLite)
dbcon <- dbConnect(RSQLite::SQLite(), "txn.db")


## Question 3 & 4

## Create products table

dbExecute(dbcon, "drop table if exists products")
createProducts <- "
CREATE TABLE if not exists products (
    pid INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name text
)"
dbExecute(dbcon, createProducts)

## Create reps table

dbExecute(dbcon, "drop table if exists reps")
createReps <- "
CREATE TABLE if not exists reps (
    rid INTEGER PRIMARY KEY AUTOINCREMENT,
    rep_firstname text,
    rep_surname text,
    territory text,
    commission number
)"
dbExecute(dbcon, createReps)

## Create customers table

dbExecute(dbcon, "drop table if exists customers ")
createCustomers <- "
CREATE TABLE if not exists customers (
    cid INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_name text,
    country text
)"
dbExecute(dbcon, createCustomers)

## Create sales table

dbExecute(dbcon, "drop table if exists sales ")
createSales <- "
CREATE TABLE if not exists sales (
    sid INTEGER PRIMARY KEY AUTOINCREMENT,
    date text,
    pid integer,
    rid integer,
    cid integer,
    quantity integer,
    total integer,
    currency text,
    foreign key (pid) references products (pid),
    foreign key (rid) references reps (rid),
    foreign key (pid) references customers (cid)
)"
dbExecute(dbcon, createSales)


## Question 5 & 6

# Create data frames
df.products <- data.frame(
  pid = integer(),
  product_name = character(),
  stringsAsFactors = FALSE
)

df.reps <- data.frame(
  rid = integer(),
  rep_firstname = character(),
  rep_surname = character(),
  territory = character(),
  commission = numeric(),
  stringsAsFactors = FALSE
)

df.customers <- data.frame(
  cid = integer(),
  customer_name = character(),
  country = character(),
  stringsAsFactors = FALSE
)

df.sales <- data.frame(
  sid = integer(),
  date = character(0),
  pid = integer(),
  rid = integer(),
  cid = integer(),
  quantity = integer(),
  total = integer(),
  currency = character(),
  stringsAsFactors = FALSE
)

## Helper function
populateReps <- function(path, file_name) {
  xmlFile <- paste(path, file_name, sep = "/")
  xmlObj <- xmlParse(xmlFile)
  
  rids <- xpathSApply(xmlObj, "//rep/@rID")
  for (rid in rids) {
    xpath <- paste0("//rep[@rID='", rid, "']")
    node <- xpathSApply(xmlObj, xpath)
    if (any(df.reps$rid == rid)) {
      next
    }
    
    rep_firstname <- xmlValue(node[[1]][[1]][[1]])
    rep_surname <- xmlValue(node[[1]][[1]][[2]])
    territory <- xmlValue(node[[1]][[2]])
    commission <- as.numeric(xmlValue(node[[1]][[3]]))

    newRow <- data.frame(rid = as.integer(substr(rid, 2, nchar(rid))), rep_firstname = rep_firstname, rep_surname = rep_surname, commission = commission, territory = territory)
    df.reps <<- rbind(df.reps, newRow)
  }
}

getCustomerID <- function(customer_name, country) {
  cid <- 0
  
  doesExist <- any(df.customers$customer_name == customer_name & df.customers$country == country)
  if (doesExist == TRUE) {
    r <- which(df.customers$customer_name == customer_name & df.customers$country == country)
    cid <- r[1]
  } else {
    cid <- nrow(df.customers) + 1
    newRow <- data.frame(cid = cid, customer_name = customer_name, country = country)
    df.customers <<- rbind(df.customers, newRow)
  }
  
  return (cid)
}

getProductID <- function(product_name) {
  pid <- 0
  
  doesExist <- any(df.products$product_name == product_name)
  if (doesExist == TRUE) {
    r <- which(df.products$product_name == product_name)
    pid <- r[1]
  } else {
    pid <- nrow(df.products) + 1
    newRow <- data.frame(pid = pid, product_name = product_name)
    df.products <<- rbind(df.products, newRow)
  }
  
  return (pid)
}

populateSales <- function(path, file_name) {
  xmlFile <- paste(path, file_name, sep = "/")
  xmlObj <- xmlParse(xmlFile)
  
  nodes <- xpathSApply(xmlObj, "//txn")
  for (node in nodes) {
    atts <- xmlAttrs(node)
    
    sid <- as.integer(atts["txnID"])
    if (any(df.sales$sid == sid)) {
      next
    }
    rid <- as.integer(atts["repID"])

    customer_name <- xmlValue(node[[1]])
    country <- xmlValue(node[[2]])
    cid <- getCustomerID(customer_name, country)
    
    product_name <- xmlValue(node[[3]][[2]])
    pid <- getProductID(product_name)
    
    date <- xmlValue(node[[3]][[1]])
    quantity <- as.integer(xmlValue(node[[3]][[3]]))
    total <- as.integer(xmlValue(node[[3]][[4]]))
    currency <- xmlAttrs(node[[3]][[4]])["currency"]
    
    # Split the date string by '/'
    date <- as.character(format(as.Date(date, "%m/%d/%Y"), "%Y-%m-%d"))
    
    newRow <- data.frame(sid = sid, rid = rid, cid = cid, pid = pid, date = date, quantity = quantity, total = total, currency = currency)
    df.sales <<- rbind(df.sales, newRow)
  }
}

## Import xml files to dataframe

path <- "./txn-xml"
files <- list.files(path=path, pattern=NULL, all.files=FALSE, full.names=FALSE)

for (file_name in files) {
  if (grepl("pharmaReps.*.xml", file_name)) {
     populateReps(path, file_name)
  }
  # if (grepl("pharmaSalesTxn-.0-F23.xml", file_name)) {
  #   populateSales(path, file_name)
  # }
  if (grepl("pharmaSalesTxn.*.xml", file_name)) {
    populateSales(path, file_name)
  }
  rownames(df.sales) <- NULL
}

## Write to database

dbWriteTable(dbcon, "products", df.products, overwrite = F, append = T, row.names = FALSE)
dbWriteTable(dbcon, "reps", df.reps, overwrite = F, append = T, row.names = FALSE)
dbWriteTable(dbcon, "customers", df.customers, overwrite = F, append = T, row.names = FALSE)
dbWriteTable(dbcon, "sales", df.sales, overwrite = F, append = T, row.names = FALSE)

# print(df.customers)
# print(df.products)
# print(df.reps)
# print(df.sales)


