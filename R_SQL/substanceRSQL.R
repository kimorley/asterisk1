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

# setwd("T:/Giulia Toti/RStudio-SQL") # set working directory to whatever folder contains the core query

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
max_diff <- 28
min_diff <- -365
max_entry_date <- "2017-05-04" # Format "%Y-%m-%d". If no limit leave ""

# The first column of the dataset should be the list of BRCIDs in the cohort.
# We can retrieve it from the NDTMS file

NDTMS <- as.data.table(NDTMS)
substance_use <- NDTMS[, .SD[which.min(entry_date)], by = BrcId]
substance_use <- substance_use[, .(BrcId, entry_date)]
substance_use$entry_date <- as.Date(substance_use$entry_date, "%Y-%m-%d", tz = "Europe/London")
setkey(substance_use, BrcId)


### ICD-10 CODES 

icd10 <- as.data.table(icd10)

icd10$Diagnosis_Date <- as.Date(icd10$Diagnosis_Date, "%Y-%m-%d", tz = "Europe/London")
icd10$entry_date <- as.Date(icd10$entry_date, "%Y-%m-%d", tz = "Europe/London")
icd10$diff <- icd10$Diagnosis_Date - icd10$entry_date 

news <- icd10[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

# Removing unused columns
news[, c("entry_date", "Diagnosis_Date", "diff") := NULL]

setkey(news, BrcId)
substance_use <- news[substance_use] # data.table syntax for right outer join


### TOP FORM: 28 DAYS SUBSTANCE USE 

TOP <- as.data.table(TOP)

TOP$TOP_Interview_Date <- as.Date(TOP$TOP_Interview_Date, "%Y-%m-%d", tz = "Europe/London")
TOP$entry_date <- as.Date(TOP$entry_date, "%Y-%m-%d", tz = "Europe/London")
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

NDTMS$Triage_Date <- as.Date(NDTMS$Triage_Date, "%Y-%m-%d", tz = "Europe/London")
NDTMS$entry_date <- as.Date(NDTMS$entry_date, "%Y-%m-%d", tz = "Europe/London")
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
Current$Todays_Date <- as.Date(Current$Todays_Date, "%Y-%m-%d", tz = "Europe/London")
Current$entry_date <- as.Date(Current$entry_date, "%Y-%m-%d", tz = "Europe/London")
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
AUDIT$A_Audit_Assessment_Date <- as.Date(AUDIT$A_Audit_Assessment_Date, "%Y-%m-%d", tz = "Europe/London")
AUDIT$entry_date <- as.Date(AUDIT$entry_date, "%Y-%m-%d", tz = "Europe/London")
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

max_entry_date <- as.Date(max_entry_date, "%Y-%m-%d", tz = "Europe/London")
substance_use <- substance_use[entry_date <= max_entry_date,]


# Saving results
today <- as.character(Sys.Date())
write.csv(substance_use, paste("substanceRSQL_",today,".csv", sep=""))


#######################################
### FUNCTIONS FOR VARIABLES SUMMARY ###
#######################################

# # Total empty rows
# ind <- apply(substance_use[,2:37], 1, function(x) all(is.na(x)))
# 
# 
# alcohol_ICD10_tot <- nrow(substance_use[like(Primary_Diag,"F10") |
#                                           like(Secondary_Diag_1,"F10") |
#                                           like(Secondary_Diag_2,"F10") |
#                                           like(Secondary_Diag_3,"F10") |
#                                           like(Secondary_Diag_4,"F10") |
#                                           like(Secondary_Diag_5,"F10") |
#                                           like(Secondary_Diag_6,"F10")])
# 
# opioids_ICD10_tot <- nrow(substance_use[like(Primary_Diag,"F11") |
#                                           like(Secondary_Diag_1,"F11") |
#                                           like(Secondary_Diag_2,"F11") |
#                                           like(Secondary_Diag_3,"F11") |
#                                           like(Secondary_Diag_4,"F11") |
#                                           like(Secondary_Diag_5,"F11") |
#                                           like(Secondary_Diag_6,"F11")])
# 
# cannabis_ICD10_tot <- nrow(substance_use[like(Primary_Diag,"F12") |
#                                            like(Secondary_Diag_1,"F12") |
#                                            like(Secondary_Diag_2,"F12") |
#                                            like(Secondary_Diag_3,"F12") |
#                                            like(Secondary_Diag_4,"F12") |
#                                            like(Secondary_Diag_5,"F12") |
#                                            like(Secondary_Diag_6,"F12")])
# 
# sedatives_ICD10_tot <- nrow(substance_use[like(Primary_Diag,"F13") |
#                                             like(Secondary_Diag_1,"F13") |
#                                             like(Secondary_Diag_2,"F13") |
#                                             like(Secondary_Diag_3,"F13") |
#                                             like(Secondary_Diag_4,"F13") |
#                                             like(Secondary_Diag_5,"F13") |
#                                             like(Secondary_Diag_6,"F13")])
# 
# cocaine_ICD10_tot <- nrow(substance_use[like(Primary_Diag,"F14") |
#                                           like(Secondary_Diag_1,"F14") |
#                                           like(Secondary_Diag_2,"F14") |
#                                           like(Secondary_Diag_3,"F14") |
#                                           like(Secondary_Diag_4,"F14") |
#                                           like(Secondary_Diag_5,"F14") |
#                                           like(Secondary_Diag_6,"F14")])
# 
# caffeine_ICD10_tot <- nrow(substance_use[like(Primary_Diag,"F15") |
#                                            like(Secondary_Diag_1,"F15") |
#                                            like(Secondary_Diag_2,"F15") |
#                                            like(Secondary_Diag_3,"F15") |
#                                            like(Secondary_Diag_4,"F15") |
#                                            like(Secondary_Diag_5,"F15") |
#                                            like(Secondary_Diag_6,"F15")])
# 
# hallucinogen_ICD10_tot <- nrow(substance_use[like(Primary_Diag,"F16") |
#                                                like(Secondary_Diag_1,"F16") |
#                                                like(Secondary_Diag_2,"F16") |
#                                                like(Secondary_Diag_3,"F16") |
#                                                like(Secondary_Diag_4,"F16") |
#                                                like(Secondary_Diag_5,"F16") |
#                                                like(Secondary_Diag_6,"F16")])
# 
# tobacco_ICD10_tot <- nrow(substance_use[like(Primary_Diag,"F17") |
#                                           like(Secondary_Diag_1,"F17") |
#                                           like(Secondary_Diag_2,"F17") |
#                                           like(Secondary_Diag_3,"F17") |
#                                           like(Secondary_Diag_4,"F17") |
#                                           like(Secondary_Diag_5,"F17") |
#                                           like(Secondary_Diag_6,"F17")])
# 
# solvents_ICD10_tot <- nrow(substance_use[like(Primary_Diag,"F18") |
#                                            like(Secondary_Diag_1,"F18") |
#                                            like(Secondary_Diag_2,"F18") |
#                                            like(Secondary_Diag_3,"F18") |
#                                            like(Secondary_Diag_4,"F18") |
#                                            like(Secondary_Diag_5,"F18") |
#                                            like(Secondary_Diag_6,"F18")])
# 
# poly_ICD10_tot <- nrow(substance_use[like(Primary_Diag,"F19") |
#                                        like(Secondary_Diag_1,"F19") |
#                                        like(Secondary_Diag_2,"F19") |
#                                        like(Secondary_Diag_3,"F19") |
#                                        like(Secondary_Diag_4,"F19") |
#                                        like(Secondary_Diag_5,"F19") |
#                                        like(Secondary_Diag_6,"F19")])
# 
# 
# #######################################
# ### CLASSIFICATION OF ALCOHOL ABUSE ###
# #######################################
# 
# 
# alcohol_use<-as.data.table(substance_use)
# # Eliminating non-alcohol related columns
# alcohol_use[,21:30] <- NULL 
# alcohol_use$entry_date <- NULL
# 
# # Summarizing list of substances in Current Drug and Alcohol
# alcohol_use$substanceCDAA <- FALSE
# alcohol_use$substanceCDAA_level <- 0
# 
# alcohol_use[Substance_1_ID == "Alcohol"]$substanceCDAA <- TRUE
# alcohol_use[Substance_1_ID == "Alcohol"]$substanceCDAA_level <- 1
# 
# alcohol_use[Substance_2_ID == "Alcohol"]$substanceCDAA <- TRUE
# alcohol_use[Substance_2_ID == "Alcohol"]$substanceCDAA_level <- 2
# 
# alcohol_use[Substance_3_ID == "Alcohol"]$substanceCDAA <- TRUE
# alcohol_use[Substance_3_ID == "Alcohol"]$substanceCDAA_level <- 3
# 
# alcohol_use[Substance_4_ID == "Alcohol"]$substanceCDAA <- TRUE
# alcohol_use[Substance_4_ID == "Alcohol"]$substanceCDAA_level <- 4
# 
# alcohol_use[Substance_5_ID == "Alcohol"]$substanceCDAA <- TRUE
# alcohol_use[Substance_5_ID == "Alcohol"]$substanceCDAA_level <- 5
# 
# alcohol_use[Substance_6_ID == "Alcohol"]$substanceCDAA <- TRUE
# alcohol_use[Substance_6_ID == "Alcohol"]$substanceCDAA_level <- 6
# 
# alcohol_use[Substance_7_ID == "Alcohol"]$substanceCDAA <- TRUE
# alcohol_use[Substance_7_ID == "Alcohol"]$substanceCDAA_level <- 7
# 
# alcohol_use[Substance_8_ID == "Alcohol"]$substanceCDAA <- TRUE
# alcohol_use[Substance_8_ID == "Alcohol"]$substanceCDAA_level <- 8
# 
# alcohol_use[Substance_9_ID == "Alcohol"]$substanceCDAA <- TRUE
# alcohol_use[Substance_9_ID == "Alcohol"]$substanceCDAA_level <- 9
# 
# alcohol_use[Substance_10_ID == "Alcohol"]$substanceCDAA <- TRUE
# alcohol_use[Substance_10_ID == "Alcohol"]$substanceCDAA_level <- 10
# 
# alcohol_use$substanceCDAA_level <- as.factor(alcohol_use$substanceCDAA_level)
# 
# 
# # Summarizing list of substances in SLAM NDTMS
# alcohol_use$substanceNDTMS <- FALSE
# alcohol_use$substanceNDTMS_level <- 0
# 
# alcohol_use[Problem_Substance1_ID == "Alcohol"]$substanceNDTMS <- TRUE
# alcohol_use[Problem_Substance1_ID == "Alcohol"]$substanceNDTMS_level <- 1
# 
# alcohol_use[Problem_Substance2_ID == "Alcohol"]$substanceNDTMS <- TRUE
# alcohol_use[Problem_Substance2_ID == "Alcohol"]$substanceNDTMS_level <- 2
# 
# alcohol_use[Problem_Substance3_ID == "Alcohol"]$substanceNDTMS <- TRUE
# alcohol_use[Problem_Substance3_ID == "Alcohol"]$substanceNDTMS_level <- 3
# 
# alcohol_use$substanceNDTMS_level <- as.factor(alcohol_use$substanceNDTMS_level)
# 
# 
# # Summarizing diagnosis (ICD-10 codes)
# alcohol_use$primary_diagnosis <- FALSE
# alcohol_use$secondary_diagnosis <- FALSE
# alcohol_use$secondary_diagnosis_level <- 0
# 
# alcohol_use[like(Primary_Diag,"F10")]$primary_diagnosis <- TRUE
# 
# alcohol_use[like(Secondary_Diag_1,"F10")]$secondary_diagnosis <- TRUE
# alcohol_use[like(Secondary_Diag_1,"F10")]$secondary_diagnosis_level <- 1
# 
# alcohol_use[like(Secondary_Diag_2,"F10")]$secondary_diagnosis <- TRUE
# alcohol_use[like(Secondary_Diag_2,"F10")]$secondary_diagnosis_level <- 2
# 
# alcohol_use[like(Secondary_Diag_3,"F10")]$secondary_diagnosis <- TRUE
# alcohol_use[like(Secondary_Diag_3,"F10")]$secondary_diagnosis_level <- 3
# 
# alcohol_use[like(Secondary_Diag_4,"F10")]$secondary_diagnosis <- TRUE
# alcohol_use[like(Secondary_Diag_4,"F10")]$secondary_diagnosis_level <- 4
# 
# alcohol_use[like(Secondary_Diag_5,"F10")]$secondary_diagnosis <- TRUE
# alcohol_use[like(Secondary_Diag_5,"F10")]$secondary_diagnosis_level <- 5
# 
# alcohol_use[like(Secondary_Diag_6,"F10")]$secondary_diagnosis <- TRUE
# alcohol_use[like(Secondary_Diag_6,"F10")]$secondary_diagnosis_level <- 6
# 
# alcohol_use$secondary_diagnosis_level <- as.factor(alcohol_use$secondary_diagnosis_level)
# 
# 
# # Consolidating other features
# alcohol_use$Alcohol_Average <- as.numeric(as.character(alcohol_use$Alcohol_Average))
# 
# alcohol_use$Audit_category <- 0 # No category/NA
# alcohol_use[like(AAudit_Risk_Cat,"ependenc")]$Audit_category <- 4
# alcohol_use[like(AAudit_Risk_Cat,"armful")]$Audit_category <- 3
# alcohol_use[like(AAudit_Risk_Cat,"ncreasing")]$Audit_category <- 2
# alcohol_use[like(AAudit_Risk_Cat,"ower")]$Audit_category <- 1
# alcohol_use[like(AAudit_Risk_Cat,"women")]$Audit_category <- 2 # special case, evaluated last
# 
# alcohol_use$Audit_category <- as.factor(alcohol_use$Audit_category)
# 
# # Sample for Nicky
# set.seed(5)
# write.csv(alcohol_use[sample(1:nrow(alcohol_use),1000),],"Alcohol_use_sample.csv")
#
# 
# cohort <- cohort[sample(1:nrow(cohort),1000),c(2,18)]