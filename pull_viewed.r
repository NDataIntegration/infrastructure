
library("RJDBC")
library("ggplot2")
library(digest)
source("pull_viewed_factory.r")


user <- 'wongie01'
pass <- 'Wong@Research40'

startDate <- as.Date("2015-01-01")
endDate   <- as.Date("2015-04-30")

drv <- JDBC("org.netezza.Driver", "C:\\JDBC\\nzjdbc.jar", "`")
conn <- dbConnect(drv, "jdbc:netezza://nantz40.nielsen.com:5480/EDW0001ADHCBPS", user, pass, believeNRows=FALSE)

dbSendUpdate(conn, getSqlSetupTemp(startDate, endDate))
result <- dbGetQuery(conn, getSqlGetViewed())

dbDisconnect(conn)

result[, 2:3] <- apply(result[, 2:3], 1:2, function(x) digest(toString(x), serialize=FALSE))


write.csv(result, file='viewed.csv', row.names=FALSE)