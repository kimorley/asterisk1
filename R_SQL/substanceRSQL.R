# This script performs the import and consolidation of
# information related to substance abuse of our cohort of 
# patients.

# Data are imported directly from SQLCRIS using the RODBC
# package. We need 5 queries to import data from 5 different
# tables:
# - Diagnosis
# - Treatment_Outcome_Profile
# - SLAM_NDTMS
# - Current_Drug_and_Alcohol
# - AUDIT

setwd("T:/Giulia Toti/RStudio-SQL") # set working directory to whatever folder contains the core query

library(RODBC)
library(data.table)

con <- odbcConnect("SQLCRIS")

############################
### 1st QUERY: DIAGNOSIS ###
############################

head_query <- "select n.BrcId, n.entry_date, d.Primary_Diag, 
d.Secondary_Diag_1, d.Secondary_Diag_2, d.Secondary_Diag_3, 
d.Secondary_Diag_4, d.Secondary_Diag_5, d.Secondary_Diag_6, 
d.Diagnosis_Date
from
("

core_query <- readLines("addiction_cohort_core_query.sql")
core_query <- paste(core_query,collapse="\n")

tail_query <- ") n	
left join
Diagnosis d
on n.BrcId = d.BrcId
where 
(Primary_Diag like 'F1%' or Secondary_Diag_1 like 'F1%'
or Secondary_Diag_1 like 'F1%' or Secondary_Diag_2 like 'F1%'
or Secondary_Diag_3 like 'F1%' or Secondary_Diag_4 like 'F1%'
or Secondary_Diag_5 like 'F1%' or Secondary_Diag_6 like 'F1%')
order by n.BrcId"

query <- paste(head_query,core_query, tail_query)
icd10 <- sqlQuery(con, query)

###########################
### 2nd QUERY: TOP FORM ###
###########################

head_query <- "select n.BrcId, n.entry_date, t.TOP_Interview_Date,
t.Alcohol_Average, t.Alcohol_Week_1_ID, t.Alcohol_Week_2_ID, t.Alcohol_Week_3_ID, t.Alcohol_Week_4_ID, t.Alcohol_Total_ID,
t.Opiates_Average, t.Opiates_Week_1_ID, t.Opiates_Week_2_ID, t.Opiates_Week_3_ID, t.Opiates_Week_4_ID, t.Opiates_Total_ID,
t.Crack_Average, t.Crack_Week_1_ID, t.Crack_Week_2_ID, t.Crack_Week_3_ID, t.Crack_Week_4_ID, t.Crack_Total_ID,
t.Cocaine_Average, t.Cocaine_Week_1_ID, t.Cocaine_Week_2_ID, t.Cocaine_Week_3_ID, t.Cocaine_Week_4_ID, t.Cocaine_Total_ID,
t.Amphetamines_Average, t.Amphetamines_Week_1_ID, t.Amphetamines_Week_2_ID, t.Amphetamines_Week_3_ID, t.Amphetamines_Week_4_ID, t.Amphetamines_Total_ID,
t.Cannabis_Average, t.Cannabis_Week_1_ID, t.Cannabis_Week_2_ID, t.Cannabis_Week_3_ID, t.Cannabis_Week_4_ID, t.Cannabis_Total_ID
from
("

core_query <- readLines("addiction_cohort_core_query.sql")
core_query <- paste(core_query,collapse="\n")

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

head_query <- "select n.BrcId, n.entry_date, sn.Problem_Substance1_ID, 
sn.Problem_Substance2_ID, sn.Problem_Substance3_ID, 
sn.Units_Of_Alcohol, sn.Drinking_Days, sn.Triage_Date
from
("

core_query <- readLines("addiction_cohort_core_query.sql")
core_query <- paste(core_query,collapse="\n")

tail_query <- ") n	
left join
SLAM_NDTMS sn
on n.BrcId = sn.BrcId
order by n.BrcId"

query <- paste(head_query,core_query, tail_query)
NDTMS <- sqlQuery(con, query)

###############################
### 4th QUERY: CURRENT DRUG ###
###############################

head_query <- "select n.BrcId, n.entry_date, sn.Substance_1_ID, sn.Substance_2_ID,
sn.Substance_3_ID, sn.Substance_4_ID, sn.Substance_5_ID, sn.Substance_6_ID,
sn.Substance_7_ID, sn.Substance_8_ID, sn.Substance_9_ID, sn.Substance_10_ID,
sn.Todays_Date
from
("

core_query <- readLines("addiction_cohort_core_query.sql")
core_query <- paste(core_query,collapse="\n")

tail_query <- ") n	
left join
Current_Drug_and_Alcohol sn
on n.BrcId = sn.BrcId
order by n.BrcId"

query <- paste(head_query,core_query, tail_query)
Current <- sqlQuery(con, query)

########################
### 5th QUERY: AUDIT ###
########################

head_query <- "select n.BrcId, n.entry_date, a.A_Audit_Assessment_Date,
a.AAudit_Total_Score, a.AAudit_Risk_Cat
from
("

core_query <- readLines("addiction_cohort_core_query.sql")
core_query <- paste(core_query,collapse="\n")

tail_query <- ") n	
left join
AUDIT a
on n.BrcId = a.BrcId
order by n.BrcId"

query <- paste(head_query,core_query, tail_query)
AUDIT <- sqlQuery(con, query)

odbcCloseAll()

#############################

# Now that we have all the data we need, we can start cleaning 
# them by date and consolidating them

# Limits for date matching 
max_diff <- 7
min_diff <- -365
max_entry_date <- "2016-12-31" # Format "%Y-%m-%d". If no limit leave ""

# The first column of the dataset should be the list of BRCIDs in the cohort.
# We can retrieve it from the NDTMS file

NDTMS <- as.data.table(NDTMS)
substance_use <- NDTMS[, .SD[which.min(entry_date)], by = BrcId]
substance_use <- substance_use[, .(BrcId, entry_date)]
substance_use$entry_date <- as.Date(substance_use$entry_date, "%Y-%m-%d")
setkey(substance_use, BrcId)


### ICD-10 CODES 

icd10 <- as.data.table(icd10)

icd10$Diagnosis_Date <- as.Date(icd10$Diagnosis_Date, "%Y-%m-%d")
icd10$entry_date <- as.Date(icd10$entry_date, "%Y-%m-%d")
icd10$diff <- icd10$Diagnosis_Date - icd10$entry_date 

news <- icd10[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

# Removing unused columns
news[, c("entry_date", "Diagnosis_Date", "diff") := NULL]

setkey(news, BrcId)
substance_use <- news[substance_use] # data.table syntax for right outer join


### TOP FORM: 28 DAYS SUBSTANCE USE 

TOP <- as.data.table(TOP)

TOP$TOP_Interview_Date <- as.Date(TOP$TOP_Interview_Date, "%Y-%m-%d")
TOP$entry_date <- as.Date(TOP$entry_date, "%Y-%m-%d")
TOP$diff <- TOP$TOP_Interview_Date - TOP$entry_date

news <- TOP[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

# The total columns have many missing values, but they can be filled using
# the weekly columns
news$Alcohol_Total_ID <- as.numeric(as.character(news$Alcohol_Week_1_ID)) + 
  as.numeric(as.character(news$Alcohol_Week_2_ID)) + 
  as.numeric(as.character(news$Alcohol_Week_3_ID)) + 
  as.numeric(as.character(news$Alcohol_Week_4_ID)) 

news$Opiates_Total_ID <- as.numeric(as.character(news$Opiates_Week_1_ID)) + 
  as.numeric(as.character(news$Opiates_Week_2_ID)) + 
  as.numeric(as.character(news$Opiates_Week_3_ID)) + 
  as.numeric(as.character(news$Opiates_Week_4_ID)) 

news$Crack_Total_ID <- as.numeric(as.character(news$Crack_Week_1_ID)) + 
  as.numeric(as.character(news$Crack_Week_2_ID)) + 
  as.numeric(as.character(news$Crack_Week_3_ID)) + 
  as.numeric(as.character(news$Crack_Week_4_ID)) 

news$Cocaine_Total_ID <- as.numeric(as.character(news$Cocaine_Week_1_ID)) + 
  as.numeric(as.character(news$Cocaine_Week_2_ID)) + 
  as.numeric(as.character(news$Cocaine_Week_3_ID)) + 
  as.numeric(as.character(news$Cocaine_Week_4_ID)) 

news$Amphetamines_Total_ID <- as.numeric(as.character(news$Amphetamines_Week_1_ID)) + 
  as.numeric(as.character(news$Amphetamines_Week_2_ID)) + 
  as.numeric(as.character(news$Amphetamines_Week_3_ID)) + 
  as.numeric(as.character(news$Amphetamines_Week_4_ID)) 

news$Cannabis_Total_ID <- as.numeric(as.character(news$Cannabis_Week_1_ID)) + 
  as.numeric(as.character(news$Cannabis_Week_2_ID)) + 
  as.numeric(as.character(news$Cannabis_Week_3_ID)) + 
  as.numeric(as.character(news$Cannabis_Week_4_ID)) 


# Preserving only BrcIds, averages and total columns
news <- news[, .(BrcId, Alcohol_Average, Alcohol_Total_ID, 
                 Opiates_Average, Opiates_Total_ID, 
                 Crack_Average, Crack_Total_ID,
                 Cocaine_Average, Cocaine_Total_ID, 
                 Amphetamines_Average, Amphetamines_Total_ID, 
                 Cannabis_Average, Cannabis_Total_ID)]

setkey(news, BrcId)
substance_use <- news[substance_use] # data.table syntax for right outer join


### NDTMS FORM

# The NDTMS table has 3 interesting sources of information: the list of 
# problem substances (up to 3), the units of alcohol consumed, and the
# drinking days (similar to TOP)

NDTMS$Triage_Date <- as.Date(NDTMS$Triage_Date, "%Y-%m-%d")
NDTMS$entry_date <- as.Date(NDTMS$entry_date, "%Y-%m-%d")
NDTMS$diff <- NDTMS$Triage_Date - NDTMS$entry_date 

news <- NDTMS[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

# Preserving columns of interest
names(news)[names(news) == 'Drinking_Days'] <- 'alcohol_total_NDTMS'
names(news)[names(news) == 'Units_Of_Alcohol'] <- 'units_of_alcohol_NDTMS'
news <- news[, .(BrcId, Problem_Substance1_ID, Problem_Substance2_ID, Problem_Substance3_ID, alcohol_total_NDTMS, units_of_alcohol_NDTMS)]

# From factor to numeric
news$alcohol_total_NDTMS <- as.numeric(as.character(news$alcohol_total_NDTMS))
news$units_of_alcohol_NDTMS <- as.numeric(as.character(news$units_of_alcohol_NDTMS))

setkey(news, BrcId)
substance_use <- news[substance_use] # data.table syntax for right outer join


### CURRENT DRUG AND ALCOHOL 

Current <- as.data.table(Current)
Current$Todays_Date <- as.Date(Current$Todays_Date, "%Y-%m-%d")
Current$entry_date <- as.Date(Current$entry_date, "%Y-%m-%d")
Current$diff <- Current$Todays_Date - Current$entry_date 

news <- Current[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

# Preserving columns of interest
news <- news[, .(BrcId, Substance_1_ID, Substance_2_ID, Substance_3_ID,
                 Substance_4_ID, Substance_5_ID, Substance_6_ID, Substance_7_ID,
                 Substance_8_ID, Substance_9_ID, Substance_10_ID)]

setkey(news, BrcId)
substance_use <- news[substance_use] # data.table syntax for right outer join


### AUDIT QUESTIONNAIRE

AUDIT <- as.data.table(AUDIT)
AUDIT$A_Audit_Assessment_Date <- as.Date(AUDIT$A_Audit_Assessment_Date, "%Y-%m-%d")
AUDIT$entry_date <- as.Date(AUDIT$entry_date, "%Y-%m-%d")
AUDIT$diff <- AUDIT$A_Audit_Assessment_Date - AUDIT$entry_date 

news <- AUDIT[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

# Preserving columns of interest
news <- news[, .(BrcId, AAudit_Total_Score, AAudit_Risk_Cat)]
news$AAudit_Total_Score <- as.numeric(as.character(news$AAudit_Total_Score))
setkey(news, BrcId)
substance_use <- news[substance_use] # data.table syntax for right outer join


# Before moving on to results consolidation, we filter out too
# recent entries (past cohort closure, max_entry_date).
# If no limit is specified the system uses today's date.
if (max_entry_date == "")
  max_entry_date <- as.character(Sys.Date())

max_entry_date <- as.Date(max_entry_date, "%Y-%m-%d")
substance_use <- substance_use[entry_date <= max_entry_date,]



# Total empty rows
ind <- apply(substance_use[,-1], 1, function(x) all(is.na(x)))

# Saving results
today <- as.character(Sys.Date())
write.csv(substance_use, paste("substanceRSQL_",today,".csv", sep=""))
