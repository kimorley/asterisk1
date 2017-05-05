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

setwd("T:/Giulia Toti/RStudio-SQL") # set working directory to whatever folder contains the core query

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
max_diff <- 7
min_diff <- -365

# The first column of the dataset should be the list of BRCIDs in the cohort.
# We can retrieve it from the NDTMS file

NDTMS <- as.data.table(NDTMS)
comorbid <- NDTMS[, .SD[which.min(entry_date)], by = BrcId]
comorbid <- comorbid[, .(BrcId)]


### NDTMS FORM ###

NDTMS$Hep_C_Latest_Test_Date <- as.Date(NDTMS$Hep_C_Latest_Test_Date, "%Y-%m-%d")
NDTMS$entry_date <- as.Date(NDTMS$entry_date, "%Y-%m-%d")
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
Blood$Last_HIV_Test_Date <- as.Date(Blood$Last_HIV_Test_Date, "%Y-%m-%d")
Blood$entry_date <- as.Date(Blood$entry_date, "%Y-%m-%d")
Blood$diff <- Blood$Last_HIV_Test_Date - Blood$entry_date 

news <- Blood[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

news <- news[, .(BrcId, HIV_Status_ID)]
setkey(news, BrcId)
comorbid <- news[comorbid] # data.table syntax for right outer join

# Then hep C
Blood$Last_HCV_Test_Date <- as.Date(Blood$Last_HCV_Test_Date, "%Y-%m-%d")
Blood$entry_date <- as.Date(Blood$entry_date, "%Y-%m-%d")
Blood$diff <- Blood$Last_HCV_Test_Date - Blood$entry_date 

news <- Blood[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

news <- news[, .(BrcId, HCV_Status_ID)]
setkey(news, BrcId)
comorbid <- news[comorbid] # data.table syntax for right outer join

# Then hep B
Blood$Last_HBV_Test_Date <- as.Date(Blood$Last_HBV_Test_Date, "%Y-%m-%d")
Blood$entry_date <- as.Date(Blood$entry_date, "%Y-%m-%d")
Blood$diff <- Blood$Last_HBV_Test_Date - Blood$entry_date 

news <- Blood[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

news <- news[, .(BrcId, HBV_Status_ID, HBV_Vaccination_Date_Course_Completed)]
setkey(news, BrcId)
comorbid <- news[comorbid] # data.table syntax for right outer join


### ICD-10 CODES HIV 

icd10 <- as.data.table(icd10)

# First we need to clean the table by date 
icd10$Diagnosis_Date <- as.Date(icd10$Diagnosis_Date, "%Y-%m-%d")
icd10$entry_date <- as.Date(icd10$entry_date, "%Y-%m-%d")
icd10$diff <- icd10$Diagnosis_Date - icd10$entry_date 

news <- icd10[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

news <- news[, .(BrcId, diff)]
colnames(news) <- c("BrcId", "HIV_ICD10")

setkey(news, BrcId)
comorbid <- news[comorbid] # data.table syntax for right outer join


### CURRENT HEALTH

curr_health <- as.data.table(curr_health)

curr_health$Last_health_check_date <- as.Date(curr_health$Last_health_check_date, "%Y-%m-%d")
curr_health$entry_date <- as.Date(curr_health$entry_date, "%Y-%m-%d")
curr_health$diff <- curr_health$Last_health_check_date - curr_health$entry_date 

news <- curr_health[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

news <- news[, .(BrcId, BMI)]

setkey(news, BrcId)
comorbid <- news[comorbid] # data.table syntax for right outer join


### CURRENT HEALTH NEW
curr_health_new <- as.data.table(curr_health_new)

curr_health_new$Date_GP_Encounter_Record_Obtained_Seen <- as.Date(curr_health_new$Date_GP_Encounter_Record_Obtained_Seen, "%Y-%m-%d")
curr_health_new$entry_date <- as.Date(curr_health_new$entry_date, "%Y-%m-%d")
curr_health_new$diff <- curr_health_new$Date_GP_Encounter_Record_Obtained_Seen - curr_health_new$entry_date 

news <- curr_health_new[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

news <- news[, .(BrcId, BMI)]
names(news)[names(news) == 'BMI'] <- 'BMI_New'

setkey(news, BrcId)
comorbid <- news[comorbid] # data.table syntax for right outer join


### FULL NUTRITION SCREEN

nutrition <- as.data.table(nutrition)

nutrition$Create_Dttm <- as.Date(nutrition$Create_Dttm, "%Y-%m-%d")
nutrition$entry_date <- as.Date(nutrition$entry_date, "%Y-%m-%d")
nutrition$diff <- nutrition$Create_Dttm - nutrition$entry_date 

news <- nutrition[diff<=max_diff & diff>=min_diff]
news <- news[, .SD[which.max(diff)], by = BrcId]

news <- news[, .(BrcId, BMI)]
names(news)[names(news) == 'BMI'] <- 'BMI_nutrition'

setkey(news, BrcId)
comorbid <- news[comorbid] # data.table syntax for right outer join


### HARM REDUCTION 

harmred <- as.data.table(harmred)
harmred$Harm_Red_Assessment_Date <- as.Date(harmred$Harm_Red_Assessment_Date, "%Y-%m-%d")
harmred$entry_date <- as.Date(harmred$entry_date, "%Y-%m-%d")
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


####################################

# Now that we have all the necessary data, we proceed to consolidate them
# into features of interest, starting from obesity.

comorbid$BMI_avg <- rowMeans(comorbid[,2:4], na.rm = TRUE)
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


# Saving results
today <- as.character(Sys.Date())
write.csv(comorbid, paste("comorbiditiesRSQL_",today,".csv", sep=""))
