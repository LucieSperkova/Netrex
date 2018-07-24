#Install the libraries
library(httr)
library(data.table)
library(dplyr)
library(xml2)

#=======BASIC INFO ABOUT THE Netrex EXTRACTOR========#

#=======CONFIGURATION========#
## initialize application
library('keboola.r.docker.application')
app <- DockerApplication$new('/data/')

app$readConfig()

## access the supplied value of 'myParameter'
PASS<-app$getParameters()$`#PASS`
USER<-app$getParameters()$USER
PARTNERID<-app$getParameters()$PARTNERID
PARTNERKEY<-app$getParameters()$`#PARTNERKEY`
granularity<-app$getParameters()$granularity
from<-app$getParameters()$from
to<-app$getParameters()$to
dataTypes <- c(
  "people_count",
  "people_count_in",
  "people_count_out"
  #,"transaction_value",
  #"transaction_count",
  #"conversion_value",
  #"conversion_count",
  #"flow_volume"
  #
)

# List of available data types
# 
# Data type	Description
# people_count	People count (average of IN and OUT).
# people_count_in	People count (IN).
# people_count_out	People count (OUT).
# transaction_value	Monetary volume of cash transactions.
# transaction_count	Number of cash transactions.
# conversion_value	Conversion ratio (related to monetary volume of cash transactions).
# conversion_count	Conversion ratio (related to number of transactions).
# flow_volume	Total flow in liters (used in liquid flow measurement application).
# List of granularities
# 
# Granularity identifier	Description
# qh	15 minutes
# 1	1 hour
# 4	4 hours
# 8	8 hours
# 12	12 hours
# d	1 day
# w	1 week
# m	1 month
# q	Quarter

##Catch config errors

if(is.null(USER)) stop("enter your username in the USER config field")
if(is.null(PASS)) stop("enter your password in the #PASS config field")
if(is.null(PARTNERID)) stop("enter your username in the PARTNERID config field")
if(is.null(PARTNERKEY)) stop("nter your username in the PARTNERKEY config field")
if(is.null(granularity)) stop("enter your password in the #PASS config field")
if(is.null(dataTypes)) stop("enter your password in the #PASS config field")
if(is.null(from)|is.null(to)) {
  warning("You entered an invalid date range, the extract will extract data from last 1 month")
  to<-Sys.Date()
  from<-to-180
  to<-as.POSIXct(to)
  from<-as.POSIXct(from)
}

##Authentication

endpoint<-"https://system.netrex.cz/APIv2/Base/EncryptPass.json"

args <-
  list(
    "USER"=USER,
    "PASS"=PASS,  
    "PARTNERID"=PARTNERID,
    "PARTNERKEY"=PARTNERKEY
  )

r <-
  RETRY(
    "GET",
    endpoint,
    times = 3,
    pause_base = 3,
    pause_cap = 10,
    query = args
  )

login<-content(r,"parsed",encoding = "UTF-8")

##Now process the request


getStats<-function(dataType){

endpoint<-"https://system.netrex.cz/APIv2/Statistics/AllData.csv"

args <-
  list(
    "granularity"=granularity,
    "dataType"=dataType,
    "from"=from,
    "to"=to,
    "USER"=USER,
    "PARTNERID"=PARTNERID,
    "ECPASS"=login
  )

r <-
  RETRY(
    "GET",
    endpoint,
    times = 3,
    pause_base = 3,
    pause_cap = 10,
    query = args
  )

res<-content(r,"text",encoding = "UTF-8")%>%textConnection%>%read.csv

}

#iterate over datatypes

for(dataType in dataTypes){
  print(paste("creating table",dataType))
  write.csv(getStats(dataType),paste("out/tables/",dataType,".csv",sep=""),row.names = FALSE)
}


