# This script performs the import and consolidation of
# the sociodemographic information of our cohort of 
# patients.

# Data are imported directly from SQLCRIS using the RODBC
# package. We need 6 queries to import data from 5 different
# tables:
# - EPR_Form
# - Brief_Risk_Screen_Addictions
# - Risk_assessment
# - Treatment_Outcome_Profile
# - SLAM_NDTMS
# - Event (for site code and location)

# setwd("T:/Giulia Toti/RStudio-SQL") # set working directory to whatever folder contains the core query

library(RODBC)
library(data.table)

con <- odbcConnect("SQLCRIS")

###########################
### 1st QUERY: EPR_FORM ###
###########################

head_query <- "select n.BrcId, n.entry_date, epr.cleaneddateofbirth, 
epr.ethnicitycleaned, epr.Gender_ID, epr.Housing_Status, epr.Occupation_ID
from
("

core_query <- readLines("addiction_cohort_core_query.sql")
core_query <- paste(core_query,collapse="\n")

tail_query <- ") n	
left join
EPR_Form epr
on n.BrcId = epr.BrcId
order by BrcId"

query <- paste(head_query,core_query, tail_query)
sociodemo <- sqlQuery(con, query)

#################################
### 2nd QUERY: BRS Addictions ###
#################################

head_query <- "select n.BrcId, n.entry_date, brs.Assessed_Date,  
brs.Social_Living_Homeless_Or_Unstable_Housing_ID
from
("

core_query <- readLines("addiction_cohort_core_query.sql")
core_query <- paste(core_query,collapse="\n")

tail_query <- ") n	
left join
Brief_Risk_Screen_Addictions brs
on n.BrcId = brs.BrcId
order by BrcId"

query <- paste(head_query,core_query, tail_query)
briefRS <- sqlQuery(con, query)

##################################
### 3rd QUERY: RISK ASSESSMENT ###
##################################

head_query <- "select n.BrcId, n.entry_date, ra.Assessed_Date, 
ra.Problems_Maintaining_Stability_In_Employment_Relationship_ID
from
("

core_query <- readLines("addiction_cohort_core_query.sql")
core_query <- paste(core_query,collapse="\n")

tail_query <- ") n	
left join
Risk_assessment ra
on n.BrcId = ra.BrcId
order by BrcId"

query <- paste(head_query,core_query, tail_query)
riskass <- sqlQuery(con, query)

###########################
### 4th QUERY: TOP FORM ###
###########################

head_query <- "select n.BrcId, n.entry_date, t.TOP_Interview_Date, t.Days_Paid_Work_Week_1_ID,
t.Days_Paid_Work_Week_2_ID, t.Days_Paid_Work_Week_3_ID, t.Days_Paid_Work_Week_4_ID,
t.Acute_Housing_Problem_ID
from
("

core_query <- readLines("addiction_cohort_core_query.sql")
core_query <- paste(core_query,collapse="\n")

tail_query <- ") n	
left join
Treatment_Outcomes_Profile t
on n.BrcId = t.BrcId
order by BrcId"

query <- paste(head_query,core_query, tail_query)
TOP <- sqlQuery(con, query)

#############################
### 5th QUERY: SLAM NDTMS ###
#############################

head_query <- "select n.BrcId, n.entry_date, s.Triage_Date, 
s.Accomodation_Need_ID, s.Employment_Status_Codes_ID
from
("

core_query <- readLines("addiction_cohort_core_query.sql")
core_query <- paste(core_query,collapse="\n")

tail_query <- ") n	
left join
SLAM_NDTMS s
on n.BrcId = s.BrcId
order by BrcId"

query <- paste(head_query,core_query, tail_query)
NDTMS <- sqlQuery(con, query)

#########################################
### 6th QUERY: COHORT DEFINITION DATA ###
#########################################

ungrouped_query <- readLines("addiction_cohort_core_query_ungrouped.sql")
ungrouped_query <- paste(ungrouped_query,collapse="\n")

event <- sqlQuery(con, ungrouped_query)

odbcCloseAll()

#############################

# Now that we have all the data we need, we can start cleaning 
# them by date and consolidating them

# Limits for date matching 
max_diff <- 28
min_diff <- -365
max_entry_date <- "2017-05-04" # Format "%Y-%m-%d". If no limit leave ""

# Begin with EPR form
sociodemo <- as.data.table(sociodemo)
sociodemo$entry_date <- as.Date(sociodemo$entry_date, "%Y-%m-%d", tz = "Europe/London")
sociodemo$cleaneddateofbirth <- as.Date(sociodemo$cleaneddateofbirth, "%Y-%m-%d", tz = "Europe/London")

setkey(sociodemo, BrcId)

# Merge with cohort definition table
event <- as.data.table(event)
news <- event[, .SD[which.min(entry_date)], by = BrcId]

# Removing unused columns
news[, "entry_date" := NULL]

setkey(news, BrcId)
sociodemo <- news[sociodemo] # data.table syntax for right outer join


# We are now going to add other columns of interest related to housing
# and emplyment status. These columns come from different tables,
# but will be consolidated to form 2 binary variables (employed, Y/N, 
# and homeless, Y/N, Y=1).

### BRIEF RISK SCREENING 
briefRS <- as.data.table(briefRS)
briefRS$Assessed_Date <- as.Date(briefRS$Assessed_Date, "%Y-%m-%d", tz = "Europe/London")
briefRS$entry_date <- as.Date(briefRS$entry_date, "%Y-%m-%d", tz = "Europe/London")
briefRS$diff <- briefRS$Assessed_Date - briefRS$entry_date 

# news <- briefRS[Social_Living_Homeless_Or_Unstable_Housing_ID != "NULL"]
# At the moment, assume no NA values for diff. If problem appears
# in the future, add
# news[is.na(diff)]$diff <- max_diff + 1 
news <- briefRS[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

# Removing unused columns
news <- news[, .(BrcId, Social_Living_Homeless_Or_Unstable_Housing_ID)]

setkey(news, BrcId)
sociodemo <- news[sociodemo] # data.table syntax for right outer join


### SLAM NDTMS 
NDTMS <- as.data.table(NDTMS)
NDTMS$Triage_Date <- as.Date(NDTMS$Triage_Date, "%Y-%m-%d", tz = "Europe/London")
NDTMS$entry_date <- as.Date(NDTMS$entry_date, "%Y-%m-%d", tz = "Europe/London")
NDTMS$diff <- NDTMS$Triage_Date - NDTMS$entry_date 

# I can't filter out NULLs because I am interested in 2 columns...
news <- NDTMS[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

# Removing unused columns
news <- news[, .(BrcId, Accomodation_Need_ID, Employment_Status_Codes_ID)]

setkey(news, BrcId)
sociodemo <- news[sociodemo] # data.table syntax for right outer join


### RISK ASSESSMENT 

riskass <- as.data.table(riskass)

riskass$Assessed_Date <- as.Date(riskass$Assessed_Date, "%Y-%m-%d", tz = "Europe/London")
riskass$entry_date <- as.Date(riskass$entry_date, "%Y-%m-%d", tz = "Europe/London")
riskass$diff <- riskass$Assessed_Date - riskass$entry_date 

# news <- riskass[Problems_Maintaining_Stability_In_Employment_Relationship_ID != "NULL"]
news <- riskass[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

# Removing unused columns
news <- news[, .(BrcId, Problems_Maintaining_Stability_In_Employment_Relationship_ID)]

setkey(news, BrcId)
sociodemo <- news[sociodemo] # data.table syntax for right outer join


### TOP FORM 

TOP <- as.data.table(TOP)
TOP$TOP_Interview_Date <- as.Date(TOP$TOP_Interview_Date, "%Y-%m-%d", tz = "Europe/London")
TOP$entry_date <- as.Date(TOP$entry_date, "%Y-%m-%d", tz = "Europe/London")
TOP$diff <- TOP$TOP_Interview_Date - TOP$entry_date 

news <- TOP[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

news$Days_Paid_Work_Week_1_ID <- as.numeric(as.character(news$Days_Paid_Work_Week_1_ID))
news$Days_Paid_Work_Week_2_ID <- as.numeric(as.character(news$Days_Paid_Work_Week_2_ID))
news$Days_Paid_Work_Week_3_ID <- as.numeric(as.character(news$Days_Paid_Work_Week_3_ID))
news$Days_Paid_Work_Week_4_ID <- as.numeric(as.character(news$Days_Paid_Work_Week_4_ID))

# Adding up weekly work days into a total column
news$Days_Paid_Work_Total <- news$Days_Paid_Work_Week_1_ID +
  news$Days_Paid_Work_Week_2_ID + news$Days_Paid_Work_Week_3_ID +
  news$Days_Paid_Work_Week_4_ID

# Removing unused columns
news <- news[, .(BrcId, Acute_Housing_Problem_ID, Days_Paid_Work_Total)]

setkey(news, BrcId)
sociodemo <- news[sociodemo] # data.table syntax for right outer join


#############################################

# Converting date of birth to age
age = function(from, to) {
  from_lt = as.POSIXlt(from)
  to_lt = as.POSIXlt(to)
  
  age = to_lt$year - from_lt$year
  
  ifelse(to_lt$mon < from_lt$mon |
           (to_lt$mon == from_lt$mon & to_lt$mday < from_lt$mday),
         age - 1, age)
}

sociodemo$Age <- age(sociodemo$cleaneddateofbirth, sociodemo$entry_date)
# sociodemo[, c("cleaneddateofbirth") := NULL]


# Before moving on to results consolidation, we filter out too
# recent entries (past cohort closure, max_entry_date).
# If no limit is specified the system uses today's date.
if (max_entry_date == "")
  max_entry_date <- as.character(Sys.Date())

max_entry_date <- as.Date(max_entry_date, "%Y-%m-%d", tz = "Europe/London")
sociodemo <- sociodemo[entry_date <= max_entry_date,]


#############################################

# Consolidation of housing features into one feature "Unstable 
# housing" with 3 levels (Y = 1, N = -1, U = 0)

# Column: housing status
# This column needs to be preprocessed first because there are many 
# categories for accommodation. We will start by setting all the
# entries at "N", then we will change homeless and unknown.
sociodemo$Housing_Status_processed <- "N"
sociodemo[sociodemo$Housing_Status=="Homeless"]$Housing_Status_processed <- "Y"
sociodemo[like(sociodemo$Housing_Status,"nown")]$Housing_Status_processed <- "U"
sociodemo[sociodemo$Housing_Status=="NULL"]$Housing_Status_processed <- "U"
sociodemo[sociodemo$Housing_Status=="Other"]$Housing_Status_processed <- "U"


sociodemo$unhousing <- 0 # Initially all patients are unknown

# First we look for signs of absence of housing problem
sociodemo[sociodemo$Acute_Housing_Problem_ID=="No"]$unhousing <- -1
sociodemo[sociodemo$Accomodation_Need_ID=="No housing problem"]$unhousing <- -1
sociodemo[sociodemo$Social_Living_Homeless_Or_Unstable_Housing_ID=="No"]$unhousing <- -1
sociodemo[sociodemo$Housing_Status_processed == "N"]$unhousing <- -1

# Then we look for signs of housing problem. This way an evidence
# of housing problem is always stronger than an evidence of 
# absence of housing problem
sociodemo[sociodemo$Acute_Housing_Problem_ID=="Yes"]$unhousing <- 1
sociodemo[sociodemo$Accomodation_Need_ID=="Housing problem"]$unhousing <- 1
sociodemo[like(sociodemo$Accomodation_Need_ID,"rgent")]$unhousing <- 1
sociodemo[sociodemo$Social_Living_Homeless_Or_Unstable_Housing_ID=="Yes"]$unhousing <- 1
sociodemo[sociodemo$Housing_Status_processed == "Y"]$unhousing <- 1

sociodemo$unhousing <- as.factor(sociodemo$unhousing)

#############################################

# Consolidation of employment features into one feature 
# "Unemployed" with 3 levels (Y = 1, N = -1, U = 0)


sociodemo$unemployed <- 0

# First we look for signs of employment
sociodemo[sociodemo$Days_Paid_Work_Total>0]$unemployed <- -1
sociodemo[sociodemo$Employment_Status_Codes_ID == "Homemaker"]$unemployed <- -1
sociodemo[sociodemo$Employment_Status_Codes_ID == "Pupil/Student"]$unemployed <- -1
sociodemo[sociodemo$Employment_Status_Codes_ID == "Retired from paid work"]$unemployed <- -1
sociodemo[sociodemo$Employment_Status_Codes_ID == "Retired from Work"]$unemployed <- -1
sociodemo[sociodemo$Employment_Status_Codes_ID == "Regular Employment"]$unemployed <- -1
sociodemo[sociodemo$Employment_Status_Codes_ID == "Unpaid voluntary work"]$unemployed <- -1
sociodemo[sociodemo$Employment_Status_Codes_ID == "Other"]$unemployed <- -1
sociodemo[sociodemo$Occupation_ID != "Not Applicable" & sociodemo$Occupation_ID != "Not Known"]$unemployed <- -1

# Then we mark unemployed subjects
sociodemo[sociodemo$Days_Paid_Work_Total==0]$unemployed <- 1
sociodemo[sociodemo$Employment_Status_Codes_ID == "Unemployed and seeking work"]$unemployed <- 1
sociodemo[sociodemo$Employment_Status_Codes_ID == "Long term sick or disabled"]$unemployed <- 1
sociodemo[sociodemo$Employment_Status_Codes_ID == "Not receiving benefit"]$unemployed <- 1

sociodemo$unemployed <- as.factor(sociodemo$unemployed)



# Total empty rows for housing and employment info
# (excluding Occupation_ID which is complete, thanks to
# the use of "Not Known" - must be a mandatory field)
ind <- apply(sociodemo[,c(2:7,10)], 1, function(x) all(is.na(x)))

# Saving results
today <- as.character(Sys.Date())
write.csv(sociodemo, paste("sociodemoRSQL_",today,".csv", sep=""))

