# This script performs the import and consolidation of
# information related to physical comorbidities in our cohort of 
# patients.

# Data are imported directly from SQLCRIS using the RODBC
# package. We are querying one table of interest: Diagnosis.

# At the moment, information on comorbidities that was possible
# to extract from CRIS include HCV, HBV, HIV, and obesity.

# Data regarding viral infections can be found in the tables:
# - SLAM NDTMS
# - Blood Born Virus
# - Diagnosis (for HIV)

# Data regarding obesity (as BMI index) can be found in:
# - Current_Physical_Health 
# - Current_Physical_Health_New
# - Full_nutrition_screen

# setwd("T:/Giulia Toti/RStudio-SQL") # set working directory to whatever folder contains the core query

library(RODBC)
library(data.table)

con <- odbcConnect("SQLCRIS")

core_query <- readLines("addiction_cohort_core_query.sql")
core_query <- paste(core_query,collapse="\n")

#############################
### 1st QUERY: SLAM NDTMS ###
#############################

head_query <- "select n.BrcId, n.entry_date, sn.Previously_Hep_B_Infected_ID, 
sn.Hep_C_Positive_ID, sn.Referred_To_Hepatology_ID, sn.Hep_B_Vaccination_Count_ID,
sn.Hep_C_Latest_Test_Date
from
("

tail_query <- ") n	
left join
SLAM_NDTMS sn
on n.BrcId = sn.BrcId
order by n.BrcId"

query <- paste(head_query,core_query, tail_query)
NDTMS <- sqlQuery(con, query)

###################################
### 2nd QUERY: BLOOD BORN VIRUS ###
###################################

head_query <- "select n.BrcId, n.entry_date, b.Last_HIV_Test_Date, b.HIV_Status_ID,
b.Last_HCV_Test_Date, b.HCV_Status_ID,
b.Last_HBV_Test_Date, b.HBV_Status_ID, b.HBV_Vaccination_Date_Course_Completed
from
("

tail_query <- ") n	
left join
Blood_Borne_Virus b
on n.BrcId = b.BrcId
order by n.BrcId"

query <- paste(head_query,core_query, tail_query)
Blood <- sqlQuery(con, query)

###################################
### 3rd QUERY: HIV ICD-10 CODES ###
###################################

head_query <- "select n.BrcId, n.entry_date, d.Diagnosis_Date
from
("

tail_query <- ") n	
left join
Diagnosis d
on n.BrcId = d.BrcId
where 
(Primary_Diag like 'B20%' or Primary_Diag like 'B21%' 
or Primary_Diag like 'B22%' or Primary_Diag like 'B23%' 
or Primary_Diag like 'B24%' or Primary_Diag like 'Z21%'
or Secondary_Diag_1 like 'B20%' or Secondary_Diag_1 like 'B21%' 
or Secondary_Diag_1 like 'B22%' or Secondary_Diag_1 like 'B23%' 
or Secondary_Diag_1 like 'B24%' or Secondary_Diag_1 like 'Z21%'
or Secondary_Diag_2 like 'B20%' or Secondary_Diag_2 like 'B21%' 
or Secondary_Diag_2 like 'B22%' or Secondary_Diag_2 like 'B23%' 
or Secondary_Diag_2 like 'B24%' or Secondary_Diag_2 like 'Z21%'
or Secondary_Diag_3 like 'B20%' or Secondary_Diag_3 like 'B21%' 
or Secondary_Diag_3 like 'B22%' or Secondary_Diag_3 like 'B23%' 
or Secondary_Diag_3 like 'B24%' or Secondary_Diag_3 like 'Z21%'
or Secondary_Diag_4 like 'B20%' or Secondary_Diag_4 like 'B21%' 
or Secondary_Diag_4 like 'B22%' or Secondary_Diag_4 like 'B23%' 
or Secondary_Diag_4 like 'B24%' or Secondary_Diag_4 like 'Z21%'
or Secondary_Diag_5 like 'B20%' or Secondary_Diag_5 like 'B21%' 
or Secondary_Diag_5 like 'B22%' or Secondary_Diag_5 like 'B23%' 
or Secondary_Diag_5 like 'B24%' or Secondary_Diag_5 like 'Z21%'
or Secondary_Diag_6 like 'B20%' or Secondary_Diag_6 like 'B21%' 
or Secondary_Diag_6 like 'B22%' or Secondary_Diag_6 like 'B23%' 
or Secondary_Diag_6 like 'B24%' or Secondary_Diag_6 like 'Z21%'
)
order by n.BrcId"

query <- paste(head_query,core_query, tail_query)
icd10 <- sqlQuery(con, query)

#################################
### 4th QUERY: CURRENT HEALTH ###
#################################

head_query <- "select n.BrcId, n.entry_date, cp.Last_health_check_date, cp.BMI
from
("

tail_query <- ") n	
left join
Current_Physical_Health cp
on n.BrcId = cp.BrcId
order by n.BrcId"

query <- paste(head_query,core_query, tail_query)
curr_health <- sqlQuery(con, query)

#####################################
### 5th QUERY: CURRENT HEALTH NEW ###
#####################################

head_query <- "select n.BrcId, n.entry_date, cp.Date_GP_Encounter_Record_Obtained_Seen, cp.BMI
from
("
  
  tail_query <- ") n	
left join
Current_Physical_Health_New cp
on n.BrcId = cp.BrcId
order by n.BrcId"
  
query <- paste(head_query,core_query, tail_query)
curr_health_new <- sqlQuery(con, query)

#################################
### 6th QUERY: FULL NUTRITION ###
#################################

head_query <- "select n.BrcId, n.entry_date, fn.Create_Dttm, fn.BMI
from
("

tail_query <- ") n	
left join
Full_nutrition_screen fn
on n.BrcId = fn.BrcId
order by n.BrcId"

query <- paste(head_query,core_query, tail_query)
nutrition <- sqlQuery(con, query)

#################################
### 7th QUERY: HARM REDUCTION ###
#################################

# This data ends up not being used because it is unstructured

head_query <- "select n.BrcId, n.entry_date, hr.Harm_Red_Assessment_Date,
hr.Sect1_HBV_Status, hr.Sect1_HCV_Status, hr.Sect1_HIV_Status
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

# The first column of the dataset should be the list of BRCIDs in the cohort.
# We can retrieve it from the NDTMS file

NDTMS <- as.data.table(NDTMS)
comorbid <- NDTMS[, .SD[which.min(entry_date)], by = BrcId]
comorbid <- comorbid[, .(BrcId, entry_date)]
comorbid$entry_date <- as.Date(comorbid$entry_date, "%Y-%m-%d", tz = "Europe/London")
setkey(comorbid, BrcId)


### NDTMS FORM ###

NDTMS$Hep_C_Latest_Test_Date <- as.Date(NDTMS$Hep_C_Latest_Test_Date, "%Y-%m-%d", tz = "Europe/London")
NDTMS$entry_date <- as.Date(NDTMS$entry_date, "%Y-%m-%d", tz = "Europe/London")
NDTMS$diff <- NDTMS$Hep_C_Latest_Test_Date - NDTMS$entry_date 

news <- NDTMS[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]


# Preserving columns of interest
news <- news[, .(BrcId, Previously_Hep_B_Infected_ID, Hep_C_Positive_ID,
                 Referred_To_Hepatology_ID, Hep_B_Vaccination_Count_ID)]
setkey(news, BrcId)
comorbid <- news[comorbid] # data.table syntax for right outer join


### BLOOD BORN VIRUS TABLE 

# Because this table includes multiple test dates, we need to do multiple
# matching

Blood <- as.data.table(Blood)

# First we check HIV data 
Blood$Last_HIV_Test_Date <- as.Date(Blood$Last_HIV_Test_Date, "%Y-%m-%d", tz = "Europe/London")
Blood$entry_date <- as.Date(Blood$entry_date, "%Y-%m-%d", tz = "Europe/London")
Blood$diff <- Blood$Last_HIV_Test_Date - Blood$entry_date 

news <- Blood[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

news <- news[, .(BrcId, HIV_Status_ID)]
setkey(news, BrcId)
comorbid <- news[comorbid] # data.table syntax for right outer join

# Then hep C
Blood$Last_HCV_Test_Date <- as.Date(Blood$Last_HCV_Test_Date, "%Y-%m-%d", tz = "Europe/London")
Blood$entry_date <- as.Date(Blood$entry_date, "%Y-%m-%d", tz = "Europe/London")
Blood$diff <- Blood$Last_HCV_Test_Date - Blood$entry_date 

news <- Blood[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

news <- news[, .(BrcId, HCV_Status_ID)]
setkey(news, BrcId)
comorbid <- news[comorbid] # data.table syntax for right outer join

# Then hep B
Blood$Last_HBV_Test_Date <- as.Date(Blood$Last_HBV_Test_Date, "%Y-%m-%d", tz = "Europe/London")
Blood$entry_date <- as.Date(Blood$entry_date, "%Y-%m-%d", tz = "Europe/London")
Blood$diff <- Blood$Last_HBV_Test_Date - Blood$entry_date 

news <- Blood[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

news <- news[, .(BrcId, HBV_Status_ID, HBV_Vaccination_Date_Course_Completed)]
setkey(news, BrcId)
comorbid <- news[comorbid] # data.table syntax for right outer join


### ICD-10 CODES HIV 

icd10 <- as.data.table(icd10)

# First we need to clean the table by date 
icd10$Diagnosis_Date <- as.Date(icd10$Diagnosis_Date, "%Y-%m-%d", tz = "Europe/London")
icd10$entry_date <- as.Date(icd10$entry_date, "%Y-%m-%d", tz = "Europe/London")
icd10$diff <- icd10$Diagnosis_Date - icd10$entry_date 

news <- icd10[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

news <- news[, .(BrcId, diff)]
colnames(news) <- c("BrcId", "HIV_ICD10")

setkey(news, BrcId)
comorbid <- news[comorbid] # data.table syntax for right outer join


### CURRENT HEALTH

curr_health <- as.data.table(curr_health)

curr_health$Last_health_check_date <- as.Date(curr_health$Last_health_check_date, "%Y-%m-%d", tz = "Europe/London")
curr_health$entry_date <- as.Date(curr_health$entry_date, "%Y-%m-%d", tz = "Europe/London")
curr_health$diff <- curr_health$Last_health_check_date - curr_health$entry_date 

news <- curr_health[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

news <- news[, .(BrcId, BMI)]

setkey(news, BrcId)
comorbid <- news[comorbid] # data.table syntax for right outer join


### CURRENT HEALTH NEW
curr_health_new <- as.data.table(curr_health_new)

curr_health_new$Date_GP_Encounter_Record_Obtained_Seen <- as.Date(curr_health_new$Date_GP_Encounter_Record_Obtained_Seen, "%Y-%m-%d", tz = "Europe/London")
curr_health_new$entry_date <- as.Date(curr_health_new$entry_date, "%Y-%m-%d", tz = "Europe/London")
curr_health_new$diff <- curr_health_new$Date_GP_Encounter_Record_Obtained_Seen - curr_health_new$entry_date 

news <- curr_health_new[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

news <- news[, .(BrcId, BMI)]
names(news)[names(news) == 'BMI'] <- 'BMI_New'

setkey(news, BrcId)
comorbid <- news[comorbid] # data.table syntax for right outer join


### FULL NUTRITION SCREEN

nutrition <- as.data.table(nutrition)

nutrition$Create_Dttm <- as.Date(nutrition$Create_Dttm, "%Y-%m-%d", tz = "Europe/London")
nutrition$entry_date <- as.Date(nutrition$entry_date, "%Y-%m-%d", tz = "Europe/London")
nutrition$diff <- nutrition$Create_Dttm - nutrition$entry_date 

news <- nutrition[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

news <- news[, .(BrcId, BMI)]
names(news)[names(news) == 'BMI'] <- 'BMI_nutrition'

setkey(news, BrcId)
comorbid <- news[comorbid] # data.table syntax for right outer join


### HARM REDUCTION 

harmred <- as.data.table(harmred)
harmred$Harm_Red_Assessment_Date <- as.Date(harmred$Harm_Red_Assessment_Date, "%Y-%m-%d", tz = "Europe/London")
harmred$entry_date <- as.Date(harmred$entry_date, "%Y-%m-%d", tz = "Europe/London")
harmred$diff <- harmred$Harm_Red_Assessment_Date - harmred$entry_date 

news <- harmred[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

news <- news[, .(BrcId, Sect1_HBV_Status, Sect1_HCV_Status, Sect1_HIV_Status)]

setkey(news, BrcId)
comorbid <- news[comorbid] # data.table syntax for right outer join



# Converting from factors to numeric
comorbid$BMI <- as.numeric(as.character(comorbid$BMI))
comorbid$BMI_New <- as.numeric(as.character(comorbid$BMI_New))
comorbid$BMI_nutrition <- as.numeric(as.character(comorbid$BMI_nutrition))


# Before moving on to results consolidation, we filter out too
# recent entries (past cohort closure, max_entry_date).
# If no limit is specified the system uses today's date.
if (max_entry_date == "")
  max_entry_date <- as.character(Sys.Date())

max_entry_date <- as.Date(max_entry_date, "%Y-%m-%d", tz = "Europe/London")
comorbid <- comorbid[entry_date <= max_entry_date,]


####################################

# Now that we have all the necessary data, we proceed to consolidate them
# into features of interest, starting from obesity.

comorbid$BMI_avg <- rowMeans(comorbid[,5:7], na.rm = TRUE)
comorbid$weight_status <- as.factor("unknown")

comorbid[BMI_avg >= 25]$weight_status <- "overweight"
comorbid[BMI_avg >= 30]$weight_status <- "obese"
comorbid[BMI_avg < 25]$weight_status <- "normoweight"
comorbid[BMI_avg < 18.5]$weight_status <- "underweight"

# HIV status
comorbid$HIV_status <- 0
comorbid[HIV_Status_ID %like% 'Neg']$HIV_status <- -1
comorbid[HIV_Status_ID %like% 'Pos' | !is.na(HIV_ICD10)]$HIV_status <- 1
comorbid$HIV_status <- as.factor(comorbid$HIV_status)

# HCV status
comorbid$HCV_status <- 0
comorbid[HCV_Status_ID %like% 'Neg' | Hep_C_Positive_ID %like% 'No']$HCV_status <- -1
comorbid[HCV_Status_ID %like% 'Pos' | Hep_C_Positive_ID %like% 'Yes']$HCV_status <- 1
comorbid$HCV_status <- as.factor(comorbid$HCV_status)

# HBV status
comorbid$HBV_status <- 0
comorbid[HBV_Status_ID %like% 'Neg' | Previously_Hep_B_Infected_ID %like% 'No']$HBV_status <- -1
comorbid[HBV_Status_ID %like% 'Pos' | Previously_Hep_B_Infected_ID %like% 'Yes']$HBV_status <- 1
comorbid$HBV_status <- as.factor(comorbid$HBV_status)

######################
### HIV CLASSIFIER ###
######################

# There is very little structured information available on our
# subjects HIV status. We are going to use a classifier (random
# forest) to predict the status of unknown patients.
library(caret)
# setwd("T:/Giulia Toti/HIVclassifier")
mod_tree <- readRDS("HIV_rf_classifier.rds")

# We are only interested in those patients which status is unknown
cohort <- comorbid[HIV_status == 0][,c(1,17)]
setkey(cohort, BrcId)

app_results <- as.data.table(read.csv("HIV306app_results_July13.csv"))
setkey(app_results, BrcId)

# Before moving further, we need to clear this results by date.
# The entry_date information is stored in the cohort table.
# Patients listed in the results but not in the cohort do not
# get assigned an entry date
app_results <- cohort[app_results]
app_results$Date <- as.Date(app_results$Date, "%d/%m/%Y", tz = "Europe/London")
app_results$entry_date <- as.Date(app_results$entry_date, "%Y-%m-%d", tz = "Europe/London")
app_results <- app_results[Date <= entry_date + 28]

# Joining tables
cohort_dia <- app_results[cohort]

# Creation of features from table
# Creating a new column for use of weighted probability
cohort_dia$mlObservation_num <- 0
cohort_dia[mlObservation1 == "negative"]$mlObservation_num <- -1
cohort_dia[mlObservation1 == "positive"]$mlObservation_num <- 1
cohort_dia$prob <- cohort_dia$prob * cohort_dia$mlObservation_num


sample <- cohort_dia[, list(total_mentions = sum(!is.na(mlObservation1)), 
                            total_positive = sum(mlObservation1 == "positive"),
                            total_negative = sum(mlObservation1 == "negative"),
                            avg_prob = mean(prob),
                            avg_MLpriority = mean(MLpriority)),
                     by = BrcId]

# Adding pos/neg ratio
sample$pn_ratio <- sample$total_positive/sample$total_negative
sample[pn_ratio == Inf]$pn_ratio <- 100 #caret does not handle Inf

# Filling in some NAs
sample$total_positive[sample$total_mentions==0] <- 0
sample$total_negative[sample$total_mentions==0] <- 0
sample$avg_prob[sample$total_mentions==0] <- 0
sample$avg_MLpriority[sample$total_mentions==0] <- 0
sample$pn_ratio[sample$total_mentions==0] <- 0


app_treatment <- as.data.table(read.csv("HIV306app_results_treatment_July13.csv", stringsAsFactors = FALSE))
setkey(app_treatment, BrcId)

# Before moving further, we need to clear this results by date.
# The entry_date information is stored in the cohort table.
# Patients listed in the results but not in the cohort do not
# get assigned an entry date
app_treatment <- cohort[app_treatment]
app_treatment$Date <- as.Date(app_treatment$Date, "%d/%m/%Y", tz = "Europe/London")
app_treatment$entry_date <- as.Date(app_treatment$entry_date, "%Y-%m-%d", tz = "Europe/London")
app_treatment <- app_treatment[Date <= entry_date + 28]

# Joining tables
cohort_treat <- app_treatment[cohort]

# Creating a new column for use of weighted probability
cohort_treat$mlObservation_num <- 0
cohort_treat[mlObservation1 == "negative"]$mlObservation_num <- -1
cohort_treat[mlObservation1 == "positive"]$mlObservation_num <- 1
cohort_treat$prob <- cohort_treat$prob * cohort_treat$mlObservation_num

sample_treat <- cohort_treat[, list(total_mentions_treat = sum(!is.na(mlObservation1)), 
                                    avg_prob_treat = mean(prob),
                                    avg_MLpriority_treat = mean(MLpriority)),
                             by = BrcId]

# Adding pos/neg ratio (in this case, TRUE if any instance is
# present)
sample_treat$pn_ratio_treat <- sample_treat$total_mentions_treat > 0

# Filling in some NAs
sample_treat$avg_prob_treat[sample_treat$total_mentions_treat==0] <- 0
sample_treat$avg_MLpriority_treat[sample_treat$total_mentions_treat==0] <- 0

# joining the two final features tables 
sample <- sample_treat[sample]

# Saving features in final table for future reference
setkey(sample, BrcId)
comorbid <- sample[comorbid]

# Some variables have a small variability, it makes sense to 
# bin them
sample$avg_prob_treat <- sample$avg_prob_treat > 0
sample$pn_ratio <- sample$pn_ratio > 50


# Predicting with the model
sample$pred <- factor(levels = c("positive", "negative"))
sample$pred <- predict(mod_tree, newdata=sample)

# Adding this new information to comorbid and updating consolidated
# results
sample <- sample[,c(1,12)]
setkey(sample, BrcId)
comorbid <- sample[comorbid]
comorbid[pred %like% 'positive']$HIV_status <- as.factor(1)
comorbid[pred %like% 'negative']$HIV_status <- as.factor(0) # We are not sure they are negative


# Saving results
# setwd("T:/Giulia Toti/RStudio-SQL") 
today <- as.character(Sys.Date())
write.csv(comorbid, paste("comorbiditiesRSQL_",today,".csv", sep=""))
