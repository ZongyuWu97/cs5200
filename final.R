

library(sqldf)

csv <- read.csv("IS2000-Customers-FinalExam.csv", header = TRUE)

res <- sqldf("select count(cid), country from csv group by country order by count(cid) asc")

print(res)