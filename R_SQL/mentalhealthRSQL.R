# This script performs the import and consolidation of
# information related to mental health diagnosis in our cohort of 
# patients.

# Data are imported directly from SQLCRIS using the RODBC
# package. We are querying one table of interest: Diagnosis.

# I think that it will be more convenient to query the table
# 3 times, separating diagnosis of psychosis, mania and depression
# at SQL level, rather than import everything at once and process
# it in R, which would result in two levels of filtering.

# setwd("T:/Giulia Toti/RStudio-SQL") # set working directory to whatever folder contains the core query

library(RODBC)
library(data.table)

con <- odbcConnect("SQLCRIS")

# Because the diagnosis table is queried using a WHERE clause that
# may exclude some BrcIds, we need to import them separately
# using the core query by itself.

core_query <- readLines("addiction_cohort_core_query.sql")
core_query <- paste(core_query,collapse="\n")

mental_health <- as.data.table(sqlQuery(con, core_query))
mental_health <- mental_health[, .(BrcId, entry_date)]
mental_health$entry_date <- as.Date(mental_health$entry_date, "%Y-%m-%d", tz = "Europe/London")
setkey(mental_health, BrcId)

#####################################
### 1st QUERY: ICD-10 - PSYCHOSIS ###
#####################################

head_query <- "select n.BrcId, n.entry_date, d.Diagnosis_Date
from
("

tail_query <- ") n	
left join
Diagnosis d
on n.BrcId = d.BrcId
where 
(Primary_Diag like 'F2%' and Primary_Diag NOT like 'F26%' and Primary_Diag NOT like 'F27%' 
or Secondary_Diag_1 like 'F2%' and Secondary_Diag_1 NOT like 'F26%' and Secondary_Diag_1 NOT like 'F27%' 
or Secondary_Diag_2 like 'F2%' and Secondary_Diag_2 NOT like 'F26%' and Secondary_Diag_2 NOT like 'F27%' 
or Secondary_Diag_3 like 'F2%' and Secondary_Diag_3 NOT like 'F26%' and Secondary_Diag_3 NOT like 'F27%' 
or Secondary_Diag_4 like 'F2%' and Secondary_Diag_4 NOT like 'F26%' and Secondary_Diag_4 NOT like 'F27%' 
or Secondary_Diag_5 like 'F2%' and Secondary_Diag_5 NOT like 'F26%' and Secondary_Diag_5 NOT like 'F27%' 
or Secondary_Diag_6 like 'F2%' and Secondary_Diag_6 NOT like 'F26%' and Secondary_Diag_6 NOT like 'F27%' 
)
order by n.BrcId"

query <- paste(head_query,core_query, tail_query)
psychosis <- sqlQuery(con, query)


#################################
### 2nd QUERY: ICD-10 - MANIA ###
#################################

head_query <- "select n.BrcId, n.entry_date, d.Diagnosis_Date
from
("

tail_query <- ") n	
left join
Diagnosis d
on n.BrcId = d.BrcId
where 
(Primary_Diag like 'F30%' or Primary_Diag like 'F31%' 
  or Secondary_Diag_1 like 'F30%' or  Secondary_Diag_1 like 'F31%' 
  or Secondary_Diag_2 like 'F30%' or  Secondary_Diag_2 like 'F31%' 
  or Secondary_Diag_3 like 'F30%' or  Secondary_Diag_3 like 'F31%' 
  or Secondary_Diag_4 like 'F30%' or  Secondary_Diag_4 like 'F31%' 
  or Secondary_Diag_5 like 'F30%' or  Secondary_Diag_5 like 'F31%' 
  or Secondary_Diag_6 like 'F30%' or  Secondary_Diag_6 like 'F31%'  
)
order by n.BrcId"

query <- paste(head_query,core_query, tail_query)
mania <- sqlQuery(con, query)


##############################################
### 3rd QUERY: ICD-10 - ANXIETY/DEPRESSION ###
##############################################

head_query <- "select n.BrcId, n.entry_date, d.Diagnosis_Date
from
("

tail_query <- ") n	
left join
Diagnosis d
on n.BrcId = d.BrcId
where 
(Primary_Diag like 'F32%' or Primary_Diag like 'F33%' 
or Primary_Diag like 'F34%' or Primary_Diag like 'F38%' 
or Primary_Diag like 'F39%' or Primary_Diag like 'F40%' 
or Primary_Diag like 'F41%' 
or Secondary_Diag_1 like 'F32%' or Secondary_Diag_1 like 'F33%' 
or Secondary_Diag_1 like 'F34%' or Secondary_Diag_1 like 'F38%' 
or Secondary_Diag_1 like 'F39%' or Secondary_Diag_1 like 'F40%' 
or Secondary_Diag_1 like 'F41%' 
or Secondary_Diag_2 like 'F32%' or Secondary_Diag_2 like 'F33%' 
or Secondary_Diag_2 like 'F34%' or Secondary_Diag_2 like 'F38%' 
or Secondary_Diag_2 like 'F39%' or Secondary_Diag_2 like 'F40%' 
or Secondary_Diag_2 like 'F41%' 
or Secondary_Diag_3 like 'F32%' or Secondary_Diag_3 like 'F33%' 
or Secondary_Diag_3 like 'F34%' or Secondary_Diag_3 like 'F38%' 
or Secondary_Diag_3 like 'F39%' or Secondary_Diag_3 like 'F40%' 
or Secondary_Diag_3 like 'F41%' 
or Secondary_Diag_4 like 'F32%' or Secondary_Diag_4 like 'F33%' 
or Secondary_Diag_4 like 'F34%' or Secondary_Diag_4 like 'F38%' 
or Secondary_Diag_4 like 'F39%' or Secondary_Diag_4 like 'F40%' 
or Secondary_Diag_4 like 'F41%' 
or Secondary_Diag_5 like 'F32%' or Secondary_Diag_5 like 'F33%' 
or Secondary_Diag_5 like 'F34%' or Secondary_Diag_5 like 'F38%' 
or Secondary_Diag_5 like 'F39%' or Secondary_Diag_5 like 'F40%' 
or Secondary_Diag_5 like 'F41%' 
or Secondary_Diag_6 like 'F32%' or Secondary_Diag_6 like 'F33%' 
or Secondary_Diag_6 like 'F34%' or Secondary_Diag_6 like 'F38%' 
or Secondary_Diag_6 like 'F39%' or Secondary_Diag_6 like 'F40%' 
or Secondary_Diag_6 like 'F41%' 
)
order by n.BrcId"

query <- paste(head_query,core_query, tail_query)
depression <- sqlQuery(con, query)

odbcCloseAll()

#############################

# Now that we have all the data we need, we can start cleaning 
# them by date and consolidating them

# Limits for date matching 
max_diff <- 28
min_diff <- -365
max_entry_date <- "2017-05-04" # Format "%Y-%m-%d". If no limit leave ""


### ICD-10 PSYCHOSIS

psychosis <- as.data.table(psychosis)

psychosis$Diagnosis_Date <- as.Date(psychosis$Diagnosis_Date, "%Y-%m-%d", tz = "Europe/London")
psychosis$entry_date <- as.Date(psychosis$entry_date, "%Y-%m-%d", tz = "Europe/London")
psychosis$diff <- psychosis$Diagnosis_Date - psychosis$entry_date 

news <- psychosis[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

# Removing unused columns
news[, c("entry_date", "Diagnosis_Date") := NULL]

setkey(news, BrcId)
mental_health <- news[mental_health] # data.table syntax for right outer join


### ICD-10 MANIA

mania <- as.data.table(mania)

mania$Diagnosis_Date <- as.Date(mania$Diagnosis_Date, "%Y-%m-%d", tz = "Europe/London")
mania$entry_date <- as.Date(mania$entry_date, "%Y-%m-%d", tz = "Europe/London")
mania$diff <- mania$Diagnosis_Date - mania$entry_date 

news <- mania[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

# Removing unused columns
news[, c("entry_date", "Diagnosis_Date") := NULL]

setkey(news, BrcId)
mental_health <- news[mental_health] # data.table syntax for right outer join


### ICD-10 ANXIETY/DEPRESSION

depression <- as.data.table(depression)

depression$Diagnosis_Date <- as.Date(depression$Diagnosis_Date, "%Y-%m-%d", tz = "Europe/London")
depression$entry_date <- as.Date(depression$entry_date, "%Y-%m-%d", tz = "Europe/London")
depression$diff <- depression$Diagnosis_Date - depression$entry_date 

news <- depression[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

# Removing unused columns
news[, c("entry_date", "Diagnosis_Date") := NULL]

setkey(news, BrcId)
mental_health <- news[mental_health] # data.table syntax for right outer join

# Reassigning names to final columns of mental_health
colnames(mental_health) <- c("BrcId", "Depression", "Mania", "Psychosis", "entry_date")

# Converting date difference (no longer used) to binary variable
mental_health[!is.na(mental_health$Psychosis)]$Psychosis <- 1
mental_health[is.na(mental_health$Psychosis)]$Psychosis <- 0
mental_health[!is.na(mental_health$Mania)]$Mania <- 1
mental_health[is.na(mental_health$Mania)]$Mania <- 0
mental_health[!is.na(mental_health$Depression)]$Depression <- 1
mental_health[is.na(mental_health$Depression)]$Depression <- 0

mental_health$Psychosis <- as.logical(mental_health$Psychosis)
mental_health$Mania <- as.logical(mental_health$Mania)
mental_health$Depression <- as.logical(mental_health$Depression)

###########################################

# Once the mental_health table has been completed, we are going to
# update it using the results of the diagnosis app, which means,
# we are going to look in these results for keywords and add
# the diagnosis to the right column.

# KEYWORDS:
# - depression --> depression_diagnosis
# - affective disorder --> depression_diagnosis
# - anxiety --> depression_diagnosis
# - psychosis/psychotic/psychoses --> psychosis_diagnosis
# - schizophrenia/schizophrenic --> psychosis_diagnosis
# - bipolar --> mania_diagnosis

# Import app results
app_diagnosis <- as.data.table(read.csv("diagnosis_outputs_final_newentry.csv"))

# Making all entries in diagnosis column lower case for easier matching
app_diagnosis$primary_diagnosis <- tolower(app_diagnosis$primary_diagnosis)

dep <- app_diagnosis[like(primary_diagnosis, "depress")]
ad <- app_diagnosis[like(primary_diagnosis, "affective disorder")]
anx <- app_diagnosis[like(primary_diagnosis, "anxiety")]
psy <- app_diagnosis[(like(primary_diagnosis, "psychos") | like(primary_diagnosis, "psychot")) & !like(primary_diagnosis, "without psycho")]
sc <- app_diagnosis[like(primary_diagnosis, "schizo")]
bp <- app_diagnosis[like(primary_diagnosis, "bipolar") | like(primary_diagnosis, "bpad")]

mental_health$BrcId <- as.character(mental_health$BrcId)
setkey(mental_health, BrcId)

# Updating Depression with new results
mental_health$Depression_app <- FALSE
mental_health$Mania_app <- FALSE
mental_health$Psychosis_app <- FALSE

dep_ind <- as.character(unique(dep$brcid))
mental_health[dep_ind]$Depression_app <- TRUE

setkey(mental_health, BrcId)
ad_ind <- as.character(unique(ad$brcid))
mental_health[ad_ind]$Depression_app <- TRUE

setkey(mental_health, BrcId)
anx_ind <- as.character(unique(anx$brcid))
mental_health[anx_ind]$Depression_app <- TRUE

# Updating Psychosis with new results
setkey(mental_health, BrcId)
psy_ind <- as.character(unique(psy$brcid))
mental_health[psy_ind]$Psychosis_app <- TRUE

setkey(mental_health, BrcId)
sc_ind <- as.character(unique(sc$brcid))
mental_health[sc_ind]$Psychosis_app <- TRUE

# Updating Mania with new results
setkey(mental_health, BrcId)
bp_ind <- as.character(unique(bp$brcid))
mental_health[bp_ind]$Mania_app <- TRUE

# Finally, we filter out too recent entries 
# (past cohort closure, max_entry_date).
# If no limit is specified the system uses today's date.
if (max_entry_date == "")
  max_entry_date <- as.character(Sys.Date())

max_entry_date <- as.Date(max_entry_date, "%Y-%m-%d", tz = "Europe/London")
mental_health <- mental_health[entry_date <= max_entry_date,]


rest <- app_diagnosis[!like(primary_diagnosis, "depress")]
rest <- rest[!like(primary_diagnosis, "affective disorder")]
rest <- rest[!like(primary_diagnosis, "anxiety")]
rest <- rest[!((like(primary_diagnosis, "psychos") | like(primary_diagnosis, "psychot")) & !like(primary_diagnosis, "without psycho"))]
rest <- rest[!like(primary_diagnosis, "schizo")]
rest <- rest[!(like(primary_diagnosis, "bipolar") | like(primary_diagnosis, "bpad"))]

# Saving results
today <- as.character(Sys.Date())
write.csv(mental_health, paste("mentalhealthRSQL_",today,".csv", sep=""))
