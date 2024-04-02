library(XML)

xmlFile <- "WuZ.CS5200.List.xml"

dom <- xmlParse(xmlFile, validate=T)

xpathEx <- "count(/to-do/Content[@name='list3'])"

res <- xpathSApply(dom, xpathEx)

print(res)

