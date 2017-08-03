# This script performs the import and consolidation of
# information related to risk behavior of our cohort of 
# patients.

# Data are imported directly from SQLCRIS using the RODBC
# package. We need 4 queries to import data from 4 different
# tables:
# - Brief_Risk_Screening_Addictions
# - Treatment_Outcome_Profile
# - SLAM_NDTMS
# - Harm_Reduction

# setwd("T:/Giulia Toti/RStudio-SQL") # set working directory to whatever folder contains the core query

library(RODBC)
library(data.table)

con <- odbcConnect("SQLCRIS")

core_query <- readLines("addiction_cohort_core_query.sql")
core_query <- paste(core_query,collapse="\n")

#################################
### 1st QUERY: BRS ADDICTIONS ###
#################################

head_query <- "select n.BrcId, n.entry_date, brs.Assessed_Date, 
brs.Health_Hight_Risk_Sexual_Behaviour_ID, brs.Treatment_Issues_Current_Injector_ID,
brs.Treatment_Issues_Previous_Injector_ID, brs.Treatment_Issues_High_Risk_Injector_ID,
brs.Treatment_Issues_Share_Injecting_Equipment_ID
from
("

tail_query <- ") n	
left join
Brief_Risk_Screen_Addictions brs
on n.BrcId = brs.BrcId
order by n.BrcId"

query <- paste(head_query,core_query, tail_query)
briefRS <- sqlQuery(con, query)

###########################
### 2nd QUERY: TOP FORM ###
###########################

head_query <- "select n.BrcId, n.entry_date, t.TOP_Interview_Date,
t.Injected_Needle_Or_Syringe_ID, t.Injected_Spoon_Water_Or_Filter_ID,
t.Injected_Total_ID, t.Sharing_ID
from
("

tail_query <- ") n	
left join
Treatment_Outcomes_Profile t
on n.BrcId = t.BrcId
order by n.BrcId"

query <- paste(head_query,core_query, tail_query)
TOP <- sqlQuery(con, query)

#############################
### 3rd QUERY: SLAM NDTMS ###
#############################

head_query <- "select n.BrcId, n.entry_date, sn.Triage_Date, sn.Ever_Shared_ID,
sn.Injected_Last_28_Days_ID, sn.Injecting_Status_ID, 
sn.Route_Of_Administration_ID, sn.Sex_Worker_ID
from
("

tail_query <- ") n	
left join
SLAM_NDTMS sn
on n.BrcId = sn.BrcId
order by n.BrcId"

query <- paste(head_query,core_query, tail_query)
NDTMS <- sqlQuery(con, query)

#################################
### 4th QUERY: HARM REDUCTION ###
#################################

head_query <- "select n.BrcId, n.entry_date, 
hr.Harm_Red_Assessment_Date, hr.Sect2_Inject1_NeverID
from
("

tail_query <- ") n	
left join
Harm_Reduction hr
on n.BrcId = hr.BrcId
order by n.BrcId"

query <- paste(head_query,core_query, tail_query)
harmred <- sqlQuery(con, query)

odbcCloseAll()

#############################

# Now that we have all the data we need, we can start cleaning 
# them by date and consolidating them

# Limits for date matching 
max_diff <- 28
min_diff <- -365
max_entry_date <- "2017-05-04" # Format "%Y-%m-%d". If no limit leave ""

# Setting up the final table that will include all data of interest:
# risk_behavior.
# The first column of the dataset should be the list of BRCIDs in the cohort.
# We can retrieve it from the briefRS table.
# we are also preserving entry date for later filtering.
briefRS <- as.data.table(briefRS)
risk_behavior <- briefRS[, .SD[which.min(entry_date)], by = BrcId]
risk_behavior <- risk_behavior[, .(BrcId, entry_date)]
risk_behavior$entry_date <- as.Date(risk_behavior$entry_date, "%Y-%m-%d", tz = "Europe/London")
setkey(risk_behavior, BrcId)


### BRIEF RISK SCREENING 

# The richest table available is Brief Risk Screening. We
# just need to clean it by date
briefRS$Assessed_Date <- as.Date(briefRS$Assessed_Date, "%Y-%m-%d", tz = "Europe/London")
briefRS$entry_date <- as.Date(briefRS$entry_date, "%Y-%m-%d", tz = "Europe/London")
briefRS$diff <- briefRS$Assessed_Date - briefRS$entry_date 

news <- briefRS[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

# Removing unused columns
news[, c("entry_date", "Assessed_Date", "diff") := NULL]

setkey(news, BrcId)
risk_behavior <- news[risk_behavior] # data.table syntax for right outer join


### SLAM NDTMS 

NDTMS <- as.data.table(NDTMS)

NDTMS$Triage_Date <- as.Date(NDTMS$Triage_Date, "%Y-%m-%d", tz = "Europe/London")
NDTMS$entry_date <- as.Date(NDTMS$entry_date, "%Y-%m-%d", tz = "Europe/London")
NDTMS$diff <- NDTMS$Triage_Date - NDTMS$entry_date 

news <- NDTMS[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

# Removing unused columns
news[, c("entry_date", "Triage_Date", "diff") := NULL]

setkey(news, BrcId)
risk_behavior <- news[risk_behavior] # data.table syntax for right outer join


### HARM REDUCTION 

harmred <- as.data.table(harmred)
harmred$Harm_Red_Assessment_Date <- as.Date(harmred$Harm_Red_Assessment_Date, "%Y-%m-%d", tz = "Europe/London")
harmred$entry_date <- as.Date(harmred$entry_date, "%Y-%m-%d", tz = "Europe/London")
harmred$diff <- harmred$Harm_Red_Assessment_Date - harmred$entry_date 

news <- harmred[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

news <- news[, .(BrcId, Sect2_Inject1_NeverID)]

setkey(news, BrcId)
risk_behavior <- news[risk_behavior] # data.table syntax for right outer join


### TOP FORM 

TOP <- as.data.table(TOP)
TOP$TOP_Interview_Date <- as.Date(TOP$TOP_Interview_Date, "%Y-%m-%d", tz = "Europe/London")
TOP$entry_date <- as.Date(TOP$entry_date, "%Y-%m-%d", tz = "Europe/London")
TOP$diff <- TOP$TOP_Interview_Date - TOP$entry_date 

news <- TOP[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

news <- news[, .(BrcId, Injected_Total_ID, Injected_Needle_Or_Syringe_ID,
                 Injected_Spoon_Water_Or_Filter_ID, Sharing_ID)]

setkey(news, BrcId)
risk_behavior <- news[risk_behavior] # data.table syntax for right outer join


# Before moving on to results consolidation, we filter out too
# recent entries (past cohort closure, max_entry_date).
# If no limit is specified the system uses today's date.
if (max_entry_date == "")
  max_entry_date <- as.character(Sys.Date())

max_entry_date <- as.Date(max_entry_date, "%Y-%m-%d", tz = "Europe/London")
risk_behavior <- risk_behavior[entry_date <= max_entry_date,]

#############################
### CONSOLIDATING RESULTS ###
#############################

# Converting factors 
risk_behavior$Injected_Total_ID <- as.numeric(as.character(risk_behavior$Injected_Total_ID))

# Total empty rows
ind <- apply(risk_behavior[,-1], 1, function(x) all(is.na(x)))

# add type column (default value = 0)
# 0 = unknown
# 1 = yes
# -1 = no
risk_behavior$injecting_now <- 0
risk_behavior$injecting_pre <- 0
risk_behavior$sharing <- 0
risk_behavior$high_risk_sex <- 0

# Checking columns related to current injecting
# We first check if the person was marked as not injecting,
# because we consider a positive to be a stronger proof, and
# this way we can change it later
risk_behavior[Injected_Total_ID==0]$injecting_now <- -1
risk_behavior[like(Injected_Needle_Or_Syringe_ID,"No")]$injecting_now <- -1
risk_behavior[like(Injected_Spoon_Water_Or_Filter_ID,"No")]$injecting_now <- -1
risk_behavior[like(Sect2_Inject1_NeverID,"Never Injected")]$injecting_now <- -1
risk_behavior[like(Injected_Last_28_Days_ID,"No")]$injecting_now <- -1
risk_behavior[like(Injecting_Status_ID,"3. Never Injected")]$injecting_now <- -1
risk_behavior[like(Treatment_Issues_Current_Injector_ID,"No")]$injecting_now <- -1
risk_behavior[like(Treatment_Issues_High_Risk_Injector_ID,"No")]$injecting_now <- -1

risk_behavior[Injected_Total_ID>0]$injecting_now <- 1
risk_behavior[like(Injected_Needle_Or_Syringe_ID,"Yes")]$injecting_now <- 1
risk_behavior[like(Injected_Spoon_Water_Or_Filter_ID,"Yes")]$injecting_now <- 1
risk_behavior[like(Sect2_Inject1_NeverID,"Current Injector")]$injecting_now <- 1
risk_behavior[like(Injected_Last_28_Days_ID,"Yes")]$injecting_now <- 1
risk_behavior[like(Injecting_Status_ID,"2. Currently injecting")]$injecting_now <- 1
risk_behavior[like(Route_Of_Administration_ID,"Inject")]$injecting_now <- 1
risk_behavior[like(Treatment_Issues_Current_Injector_ID,"Yes")]$injecting_now <- 1
risk_behavior[like(Treatment_Issues_High_Risk_Injector_ID,"Yes")]$injecting_now <- 1

# Checking columns related to previously injecting
risk_behavior[like(Sect2_Inject1_NeverID,"Never Injected")]$injecting_pre <- -1
risk_behavior[like(Injecting_Status_ID,"3. Never Injected")]$injecting_pre <- -1
risk_behavior[like(Treatment_Issues_Previous_Injector_ID,"No")]$injecting_pre <- -1

risk_behavior[like(Sect2_Inject1_NeverID,"Previous Injector")]$injecting_pre <- 1
risk_behavior[like(Injecting_Status_ID,"1. Previously injected")]$injecting_pre <- 1
risk_behavior[like(Treatment_Issues_Previous_Injector_ID,"Yes")]$injecting_pre <- 1

# Checking columns related to sharing
risk_behavior[like(Sharing_ID,"No")]$sharing <- -1
risk_behavior[like(Ever_Shared_ID,"No")]$sharing <- -1
risk_behavior[like(Treatment_Issues_Share_Injecting_Equipment_ID,"No")]$sharing <- -1

risk_behavior[like(Sharing_ID,"Yes")]$sharing <- 1
risk_behavior[like(Ever_Shared_ID,"Yes")]$sharing <- 1
risk_behavior[like(Treatment_Issues_Share_Injecting_Equipment_ID,"Yes")]$sharing <- 1

# Checking columns related to high risk sexual behavior
# risk_behavior[like(Sex_Worker_ID,"Not a sex worker")]$high_risk_sex <- -1
risk_behavior[like(Health_Hight_Risk_Sexual_Behaviour_ID,"No")]$high_risk_sex <- -1

risk_behavior[Sex_Worker_ID %like% "^Selling sex"]$high_risk_sex <- 1
risk_behavior[like(Health_Hight_Risk_Sexual_Behaviour_ID,"Yes")]$high_risk_sex <- 1

risk_behavior$injecting_now <- as.factor(risk_behavior$injecting_now)
risk_behavior$injecting_pre <- as.factor(risk_behavior$injecting_pre)
risk_behavior$sharing <- as.factor(risk_behavior$sharing)
risk_behavior$high_risk_sex <- as.factor(risk_behavior$high_risk_sex)


# Total empty rows
ind <- apply(risk_behavior[,2:16], 1, function(x) all(is.na(x)))

# Saving results
today <- as.character(Sys.Date())
write.csv(risk_behavior, paste("riskbehaviorsRSQL_",today,".csv", sep=""))
