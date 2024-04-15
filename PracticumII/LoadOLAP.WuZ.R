## Zongyu WU, 24SP CS5200, 4/13/2024



## Connect to database

library("RMariaDB")
library("DBI")

db_user <- 'admin'            # use your value from the setup
db_password <- 'wuzongyu'    # use your value from the setup
db_name <- 'practicum1'         # use your value from the setup

db_host <-
  'cs5200.ceko8sdfannu.us-east-1.rds.amazonaws.com'       # for aws

db_port <- 3306

db.sqlite <- dbConnect(RSQLite::SQLite(), "txn.db")
db.aws <-
  DBI::dbConnect(
    RMariaDB::MariaDB(),
    user = db_user,
    password = db_password,
    dbname = db_name,
    host = db_host,
    port = db_port
  )

## Question 3

tryCatch({
  ## Create product_facts table
  dbExecute(db.aws, "drop table if exists product_facts ")
  createProductFacts <- "
    CREATE TABLE if not exists product_facts (
        pfid INTEGER PRIMARY KEY AUTO_INCREMENT,
        product_name TEXT,
        total_amount_sold INTEGER,
        quarter INTEGER,
        year INTEGER,
        total_unit INTEGER,
        region text
    )"
  dbExecute(db.aws, createProductFacts)
  
  ## Populate product_facts table
  productFact <- "
    SELECT
        p.product_name,
        SUM(s.total) AS total_amount_sold,
        (STRFTIME('%m', s.date) + 2) / 3 AS quarter,
        STRFTIME('%Y', s.date) AS year,
        sum(s.quantity) as total_unit,
        r.territory AS region
    FROM
        sales s
    JOIN
        products p ON s.pid = p.pid
    JOIN
        reps r on s.rid = r.rid
    GROUP BY
        p.product_name, (STRFTIME('%m', s.date) + 2) / 3, STRFTIME('%Y', s.date), r.territory
"
  productRes <- dbGetQuery(db.sqlite, productFact)
  
  
  ## Create rep_facts table
  dbExecute(db.aws, "drop table if exists rep_facts ")
  createRepFact <- "
    CREATE TABLE if not exists rep_facts (
        rfid INTEGER PRIMARY KEY AUTO_INCREMENT,
        rep_name text,
        total_amount_sold integer,
        average_amount_sold numeric,
        quarter integer,
        year integer
    )"
  dbExecute(db.aws, createRepFact)
  
  ## Populate rep_facts table
  repFact <- "
    SELECT 
        CONCAT(r.rep_firstname, ' ', r.rep_surname) AS rep_name,
        SUM(s.total) AS total_amount_sold,
        AVG(s.total) AS average_amount_sold,
        (STRFTIME('%m', s.date) + 2) / 3 AS quarter,
        STRFTIME('%Y', s.date) AS year
    FROM 
        sales s
    JOIN 
        reps r ON s.rid = r.rid
    GROUP BY 
        rep_name, (STRFTIME('%m', s.date) + 2) / 3, STRFTIME('%Y', s.date)
"
  repRes <- dbGetQuery(db.sqlite, repFact)
  
  
  ## Write into MySQL
  
  dbWriteTable(db.aws, "product_facts", productRes, overwrite = F, append = T, row.names = FALSE)
  dbWriteTable(db.aws, "rep_facts", repRes, overwrite = F, append = T, row.names = FALSE)
  
},

finally = {
  dbDisconnect(db.sqlite)
  dbDisconnect(db.aws)
})
# INSERT INTO rep_facts (rep_name, total_amount_sold, average_amount_sold_per_quarter, average_amount_sold_per_year)

# INSERT INTO product_facts (product_name, total_amount_sold, total_amount_sold_quarter, total_amount_sold_year, total_units_per_region)
