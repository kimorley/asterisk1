# In this script we source all the RSQL files and merge the results in 
# a final large table

setwd("T:/Giulia Toti/ASTERISK cohort")

source('comorbiditiesRSQL.R')
source('mentalhealthRSQL.R')
source('riskbehaviorsRSQL.R')
source('sociodemoRSQL.R')
source('substanceRSQL.R')

# All of these table have an entry date column that is needed only once
comorbid[, c("entry_date") := NULL]
mental_health[, c("entry_date") := NULL]
risk_behavior[, c("entry_date") := NULL]
substance_use[, c("entry_date") := NULL]

mental_health$BrcId <- as.numeric(mental_health$BrcId)
setkey(mental_health, BrcId)

# merging all together
CRISfinal <- comorbid[mental_health]
CRISfinal <- risk_behavior[CRISfinal]
CRISfinal <- substance_use[CRISfinal]
CRISfinal <- sociodemo[CRISfinal]

# save results
today <- as.character(Sys.Date())
write.csv(CRISfinal, paste("CRISfinal_",today,".csv", sep=""))

# Use this line if you wish to import a previously created table,
# otherwise comment it

library(data.table)

CRISfinal <- read.csv("CRISfinal_2017-07-14.csv")
CRISfinal <- as.data.table(CRISfinal[,-1]) # First column is indexes


# LOCATIONS
# CLINIC LOCATIONS
# NOTES ON PROCESSING:
# clean-up of location names that don't follow format
# AAU: seems to be acute assessment unit of Maudsley Hospital
# Alcohol: Assertive Outreach Team not technically a DAT as in-home visit and part of pilot so exclude
# IOT: seem to be clinics at Southwark or Maudsley, but need to get this from end of string
# National: This is the Maudsley in all cases
# SM: This is also the Maudsley in all cases
# Virtual: virtual outpatients - not sure how to assign this so may have to DELETE
#-------------------------------------------
# NOTE THAT LBC = LAMBETH (LHH AND HARBOUR)
# BOROUGHS: Croydon, Lambeth, Lewisham, Southwark BUT DATS in others
# NOTE: Maudsley is in Southwark

require(car)

location <- c()

for (i in 1:nrow(CRISfinal)){
  temp1 <- trimws(unlist(strsplit(as.character(CRISfinal$Location_Name[i]), "-"))[2])
  temp2 <- unlist(strsplit(temp1, " "))[1]
  
  if (temp2=='IOT'){
    temp2 <- trimws(unlist(strsplit(temp1, " "))[4]) 
  }
  
  if (temp2 %in% c('National','SM','AAU','Maudsley')){
    temp2 <- 'Southwark'
  } 
  else if (temp2 %in% c('Alcohol','Virtual')){
    temp2 <- 'REMOVE'
  }
  
  if (temp2=='LBC'){
    temp2 <- 'Lambeth'
  }
  
  location <- c(location, temp2)
}


CRISfinal$clinicLocation <- location

# with(CRISfinal, table(clinicLocation, useNA='ifany'))

# ETHNICITY
ethnicity<-as.data.table(CRISfinal$ethnicitycleaned)
ethnicity[V1 == "Not Stated (Z)"] <- "Unknown"
ethnicity[V1 == "British (A)"] <- "White"
ethnicity[V1 == "Irish (B)"] <- "White"
ethnicity[V1 == "Any other white background (C)"] <- "White"
ethnicity[V1 == "White and black Caribbean (D)"] <- "Mixed"
ethnicity[V1 == "White and Black African (E)"] <- "Mixed"
ethnicity[V1 == "White and Asian (F)"] <- "Mixed"
ethnicity[V1 == "Any other mixed background (G)"] <- "Mixed"
ethnicity[V1 == "Indian (H)"] <- "Asian"
ethnicity[V1 == "Pakistani (J)"] <- "Asian"
ethnicity[V1 == "Bangladeshi (K)"] <- "Asian"
ethnicity[V1 == "Chinese (R)"] <- "Asian"
ethnicity[V1 == "Any other Asian background (L)"] <- "Asian"
ethnicity[V1 == "Caribbean (M)"] <- "Black"
ethnicity[V1 == "African (N)"] <- "Black"
ethnicity[V1 == "Any other black background (P)"] <- "Black"
ethnicity[V1 == "Any other ethnic group (S)"] <- "Other"
ethnicity[is.na(ethnicity)] <- "Unknown"
ethnicity <- droplevels(ethnicity)
CRISfinal$ethnicity <- ethnicity


# LIVING SITUATION
CRISfinal$livingSituation <- factor(levels = c("Stable","Unstable","Unknown"))
CRISfinal$livingSituation <- "Unknown"
CRISfinal[unhousing == 1]$livingSituation <- "Unstable"
CRISfinal[unhousing == -1]$livingSituation <- "Stable"


# EMPLOYMENT
CRISfinal$employmentStatus <- factor(levels = c("Employed","Unemployed","Unknown"))
CRISfinal$employmentStatus <- "Unknown"
CRISfinal[unemployed == 1]$employmentStatus <- "Unemployed"
CRISfinal[unemployed == -1]$employmentStatus <- "Employed"


# ALCOHOL USE
# We are using a classifier to group patients according to their alcohol
# use. The classifier can not use all of the raw features available, 
# therefore we need to create some.
# NOTE: I am recreating all the features tested in Alcohol_classifier.R,
# even though at the end of that script only 4 features were selected:
# 1- substanceCDAA (binary)
# 2- substanceNDTMS (binary)
# 3- substanceNDTMS_level
# 4- primary_diagnosis (binary)
# 5- secondary_diagnosis (binary)
# 6- substanceCDAA_level
# 7- AAudit_Total_Score
# 8- Audit_category (factor: 0 (missing) to 4)
# 9- units_of_alcohol_NDTMS
# 10- alcohol_total_NDTMS
# 11- Alcohol_Average
# 12- Alcohol_Total_ID

CRISfinal$substanceCDAA <- FALSE
CRISfinal[Substance_1_ID %in% c("Alcohol", "alcohol")]$substanceCDAA <- TRUE
CRISfinal[Substance_2_ID %in% c("Alcohol", "alcohol")]$substanceCDAA <- TRUE
CRISfinal[Substance_3_ID %in% c("Alcohol", "alcohol")]$substanceCDAA <- TRUE
CRISfinal[Substance_4_ID %in% c("Alcohol", "alcohol")]$substanceCDAA <- TRUE
CRISfinal[Substance_5_ID %in% c("Alcohol", "alcohol")]$substanceCDAA <- TRUE
CRISfinal[Substance_6_ID %in% c("Alcohol", "alcohol")]$substanceCDAA <- TRUE
CRISfinal[Substance_7_ID %in% c("Alcohol", "alcohol")]$substanceCDAA <- TRUE
CRISfinal[Substance_8_ID %in% c("Alcohol", "alcohol")]$substanceCDAA <- TRUE
CRISfinal[Substance_9_ID %in% c("Alcohol", "alcohol")]$substanceCDAA <- TRUE
CRISfinal[Substance_10_ID %in% c("Alcohol", "alcohol")]$substanceCDAA <- TRUE

CRISfinal$substanceNDTMS <- FALSE
CRISfinal[Problem_Substance1_ID %in% c("Alcohol", "alcohol")]$substanceNDTMS <- TRUE
CRISfinal[Problem_Substance2_ID %in% c("Alcohol", "alcohol")]$substanceNDTMS <- TRUE
CRISfinal[Problem_Substance3_ID %in% c("Alcohol", "alcohol")]$substanceNDTMS <- TRUE

CRISfinal$substanceNDTMS_level <- 0 #missing
CRISfinal[Problem_Substance1_ID %in% c("Alcohol", "alcohol")]$substanceNDTMS_level <- 1
CRISfinal[Problem_Substance2_ID %in% c("Alcohol", "alcohol")]$substanceNDTMS_level <- 2
CRISfinal[Problem_Substance3_ID %in% c("Alcohol", "alcohol")]$substanceNDTMS_level <- 3
CRISfinal$substanceNDTMS_level <- as.factor(CRISfinal$substanceNDTMS_level)

CRISfinal$primary_diagnosis <- FALSE
CRISfinal[like(Primary_Diag,"F10")]$primary_diagnosis <- TRUE

CRISfinal$secondary_diagnosis <- FALSE
CRISfinal[like(Secondary_Diag_1,"F10")]$secondary_diagnosis <- TRUE
CRISfinal[like(Secondary_Diag_2,"F10")]$secondary_diagnosis <- TRUE
CRISfinal[like(Secondary_Diag_3,"F10")]$secondary_diagnosis <- TRUE
CRISfinal[like(Secondary_Diag_4,"F10")]$secondary_diagnosis <- TRUE
CRISfinal[like(Secondary_Diag_5,"F10")]$secondary_diagnosis <- TRUE
CRISfinal[like(Secondary_Diag_6,"F10")]$secondary_diagnosis <- TRUE

CRISfinal$substanceCDAA_level <- 0
CRISfinal[Substance_1_ID %in% c("Alcohol", "alcohol")]$substanceCDAA_level <- 1
CRISfinal[Substance_2_ID %in% c("Alcohol", "alcohol")]$substanceCDAA_level <- 2
CRISfinal[Substance_3_ID %in% c("Alcohol", "alcohol")]$substanceCDAA_level <- 3
CRISfinal[Substance_4_ID %in% c("Alcohol", "alcohol")]$substanceCDAA_level <- 4
CRISfinal[Substance_5_ID %in% c("Alcohol", "alcohol")]$substanceCDAA_level <- 5
CRISfinal[Substance_6_ID %in% c("Alcohol", "alcohol")]$substanceCDAA_level <- 6
CRISfinal[Substance_7_ID %in% c("Alcohol", "alcohol")]$substanceCDAA_level <- 7
CRISfinal[Substance_8_ID %in% c("Alcohol", "alcohol")]$substanceCDAA_level <- 8
CRISfinal[Substance_9_ID %in% c("Alcohol", "alcohol")]$substanceCDAA_level <- 9
CRISfinal[Substance_10_ID %in% c("Alcohol", "alcohol")]$substanceCDAA_level <- 10

# AAudit_Total_Score is ready

CRISfinal$Audit_category <- 0 # No category/NA
CRISfinal[like(AAudit_Risk_Cat,"ependenc")]$Audit_category <- 4
CRISfinal[like(AAudit_Risk_Cat,"armful")]$Audit_category <- 3
CRISfinal[like(AAudit_Risk_Cat,"ncreasing")]$Audit_category <- 2
CRISfinal[like(AAudit_Risk_Cat,"ower")]$Audit_category <- 1
CRISfinal[like(AAudit_Risk_Cat,"women")]$Audit_category <- 2 # special case, evaluated last
CRISfinal$Audit_category <- as.factor(CRISfinal$Audit_category)

# units_of_alcohol_NDTMS is ready

# alcohol_total_NDTMS is ready

CRISfinal$Alcohol_Average <- as.numeric(as.character(CRISfinal$Alcohol_Average))

# Alcohol_Total_ID is ready

# # This features can now be used as input for the classifier of choice
# # to generate the new variable 
# # These lines are commented because at the moment the classifier
# # is not able to handle rows with missing data
# mod_svm <- readRDS("Alcohol_svm_classifier.rds")
# mod_rf <- readRDS("Alcohol_rf_classifier.rds")
# 
# temp <- predict(mod_svm, newdata=CRISfinal)
# temp2 <- predict(mod_rf, newdata=CRISfinal)


# OPIATE USE
CRISfinal$opiateUse <- FALSE
CRISfinal[Opiates_Total_ID > 0]$opiateUse <- TRUE
CRISfinal[like(Primary_Diag, "F11")]$opiateUse <- TRUE
CRISfinal[like(Secondary_Diag_1, "F11")]$opiateUse <- TRUE
CRISfinal[like(Secondary_Diag_2, "F11")]$opiateUse <- TRUE
CRISfinal[like(Secondary_Diag_3, "F11")]$opiateUse <- TRUE
CRISfinal[like(Secondary_Diag_4, "F11")]$opiateUse <- TRUE
CRISfinal[like(Secondary_Diag_5, "F11")]$opiateUse <- TRUE
CRISfinal[like(Secondary_Diag_6, "F11")]$opiateUse <- TRUE

opioids_keywords = c("Heroin", "Other Opiates", "Dihydrocodeine",
                     "Fentanyl", "Physeptone", "Opium", "Codeine")
CRISfinal[Substance_1_ID %in% opioids_keywords]$opiateUse <- TRUE
CRISfinal[Substance_2_ID %in% opioids_keywords]$opiateUse <- TRUE
CRISfinal[Substance_3_ID %in% opioids_keywords]$opiateUse <- TRUE
CRISfinal[Substance_4_ID %in% opioids_keywords]$opiateUse <- TRUE
CRISfinal[Substance_5_ID %in% opioids_keywords]$opiateUse <- TRUE
CRISfinal[Substance_6_ID %in% opioids_keywords]$opiateUse <- TRUE
CRISfinal[Substance_7_ID %in% opioids_keywords]$opiateUse <- TRUE
CRISfinal[Substance_8_ID %in% opioids_keywords]$opiateUse <- TRUE
CRISfinal[Substance_9_ID %in% opioids_keywords]$opiateUse <- TRUE
CRISfinal[Substance_10_ID %in% opioids_keywords]$opiateUse <- TRUE
CRISfinal[Problem_Substance1_ID %in% opioids_keywords]$opiateUse <- TRUE
CRISfinal[Problem_Substance2_ID %in% opioids_keywords]$opiateUse <- TRUE
CRISfinal[Problem_Substance3_ID %in% opioids_keywords]$opiateUse <- TRUE


# METHADONE USE
CRISfinal$mbUse <- FALSE
mb_keywords = c("Methadone", "Methodone", "Buprenorphine")
CRISfinal[Substance_1_ID %in% mb_keywords]$mbUse <- TRUE
CRISfinal[Substance_2_ID %in% mb_keywords]$mbUse <- TRUE
CRISfinal[Substance_3_ID %in% mb_keywords]$mbUse <- TRUE
CRISfinal[Substance_4_ID %in% mb_keywords]$mbUse <- TRUE
CRISfinal[Substance_5_ID %in% mb_keywords]$mbUse <- TRUE
CRISfinal[Substance_6_ID %in% mb_keywords]$mbUse <- TRUE
CRISfinal[Substance_7_ID %in% mb_keywords]$mbUse <- TRUE
CRISfinal[Substance_8_ID %in% mb_keywords]$mbUse <- TRUE
CRISfinal[Substance_9_ID %in% mb_keywords]$mbUse <- TRUE
CRISfinal[Substance_10_ID %in% mb_keywords]$mbUse <- TRUE
CRISfinal[Problem_Substance1_ID %in% mb_keywords]$mbUse <- TRUE
CRISfinal[Problem_Substance2_ID %in% mb_keywords]$mbUse <- TRUE
CRISfinal[Problem_Substance3_ID %in% mb_keywords]$mbUse <- TRUE


# COCAINE USE
CRISfinal$cocaineUse <- FALSE
CRISfinal[Cocaine_Total_ID > 0]$cocaineUse <- TRUE
CRISfinal[Crack_Total_ID > 0]$cocaineUse <- TRUE
CRISfinal[like(Primary_Diag, "F14")]$cocaineUse <- TRUE
CRISfinal[like(Secondary_Diag_1, "F14")]$cocaineUse <- TRUE
CRISfinal[like(Secondary_Diag_2, "F14")]$cocaineUse <- TRUE
CRISfinal[like(Secondary_Diag_3, "F14")]$cocaineUse <- TRUE
CRISfinal[like(Secondary_Diag_4, "F14")]$cocaineUse <- TRUE
CRISfinal[like(Secondary_Diag_5, "F14")]$cocaineUse <- TRUE
CRISfinal[like(Secondary_Diag_6, "F14")]$cocaineUse <- TRUE

cocaine_keywords = c("Cocaine", "Cocaine Hydrochloride", "Crack Cocaine")
CRISfinal[Substance_1_ID %in% cocaine_keywords]$cocaineUse <- TRUE
CRISfinal[Substance_2_ID %in% cocaine_keywords]$cocaineUse <- TRUE
CRISfinal[Substance_3_ID %in% cocaine_keywords]$cocaineUse <- TRUE
CRISfinal[Substance_4_ID %in% cocaine_keywords]$cocaineUse <- TRUE
CRISfinal[Substance_5_ID %in% cocaine_keywords]$cocaineUse <- TRUE
CRISfinal[Substance_6_ID %in% cocaine_keywords]$cocaineUse <- TRUE
CRISfinal[Substance_7_ID %in% cocaine_keywords]$cocaineUse <- TRUE
CRISfinal[Substance_8_ID %in% cocaine_keywords]$cocaineUse <- TRUE
CRISfinal[Substance_9_ID %in% cocaine_keywords]$cocaineUse <- TRUE
CRISfinal[Substance_10_ID %in% cocaine_keywords]$cocaineUse <- TRUE
CRISfinal[Problem_Substance1_ID %in% cocaine_keywords]$cocaineUse <- TRUE
CRISfinal[Problem_Substance2_ID %in% cocaine_keywords]$cocaineUse <- TRUE
CRISfinal[Problem_Substance3_ID %in% cocaine_keywords]$cocaineUse <- TRUE


# BENZODIAZEPINE USE
CRISfinal$benzoUse <- FALSE

benzo_keywords = c("Benzodiazepam", "Diazepam", "Alprazolam")
CRISfinal[Substance_1_ID %in% benzo_keywords]$benzoUse <- TRUE
CRISfinal[Substance_2_ID %in% benzo_keywords]$benzoUse <- TRUE
CRISfinal[Substance_3_ID %in% benzo_keywords]$benzoUse <- TRUE
CRISfinal[Substance_4_ID %in% benzo_keywords]$benzoUse <- TRUE
CRISfinal[Substance_5_ID %in% benzo_keywords]$benzoUse <- TRUE
CRISfinal[Substance_6_ID %in% benzo_keywords]$benzoUse <- TRUE
CRISfinal[Substance_7_ID %in% benzo_keywords]$benzoUse <- TRUE
CRISfinal[Substance_8_ID %in% benzo_keywords]$benzoUse <- TRUE
CRISfinal[Substance_9_ID %in% benzo_keywords]$benzoUse <- TRUE
CRISfinal[Substance_10_ID %in% benzo_keywords]$benzoUse <- TRUE
CRISfinal[Problem_Substance1_ID %in% benzo_keywords]$benzoUse <- TRUE
CRISfinal[Problem_Substance2_ID %in% benzo_keywords]$benzoUse <- TRUE
CRISfinal[Problem_Substance3_ID %in% benzo_keywords]$benzoUse <- TRUE


# CANNABIS USE
CRISfinal$cannUse <- FALSE
CRISfinal[Cannabis_Total_ID > 0]$cannUse <- TRUE
CRISfinal[like(Primary_Diag, "F12")]$cannUse <- TRUE
CRISfinal[like(Secondary_Diag_1, "F12")]$cannUse <- TRUE
CRISfinal[like(Secondary_Diag_2, "F12")]$cannUse <- TRUE
CRISfinal[like(Secondary_Diag_3, "F12")]$cannUse <- TRUE
CRISfinal[like(Secondary_Diag_4, "F12")]$cannUse <- TRUE
CRISfinal[like(Secondary_Diag_5, "F12")]$cannUse <- TRUE
CRISfinal[like(Secondary_Diag_6, "F12")]$cannUse <- TRUE

cann_keywords = c("Cannabis", "Cannabis Herbal (Skunk)")
CRISfinal[Substance_1_ID %in% cann_keywords]$cannUse <- TRUE
CRISfinal[Substance_2_ID %in% cann_keywords]$cannUse <- TRUE
CRISfinal[Substance_3_ID %in% cann_keywords]$cannUse <- TRUE
CRISfinal[Substance_4_ID %in% cann_keywords]$cannUse <- TRUE
CRISfinal[Substance_5_ID %in% cann_keywords]$cannUse <- TRUE
CRISfinal[Substance_6_ID %in% cann_keywords]$cannUse <- TRUE
CRISfinal[Substance_7_ID %in% cann_keywords]$cannUse <- TRUE
CRISfinal[Substance_8_ID %in% cann_keywords]$cannUse <- TRUE
CRISfinal[Substance_9_ID %in% cann_keywords]$cannUse <- TRUE
CRISfinal[Substance_10_ID %in% cann_keywords]$cannUse <- TRUE
CRISfinal[Problem_Substance1_ID %in% cann_keywords]$cannUse <- TRUE
CRISfinal[Problem_Substance2_ID %in% cann_keywords]$cannUse <- TRUE
CRISfinal[Problem_Substance3_ID %in% cann_keywords]$cannUse <- TRUE


# MENTAL HEALTH
CRISfinal$mhPsyc <- CRISfinal$Psychosis | CRISfinal$Psychosis_app
CRISfinal$mhAnxDep <- CRISfinal$Depression | CRISfinal$Depression_app
CRISfinal$mhMania <- CRISfinal$Mania | CRISfinal$Mania_app


# INJECTING DRUG USE
CRISfinal$rbIDU <- factor(levels = c("Currently","Previously", "Never", "Unknown"))
CRISfinal$rbIDU <- "Unknown"
CRISfinal[injecting_pre == 1]$rbIDU <- "Previously"
CRISfinal[injecting_now == 1]$rbIDU <- "Currently"
CRISfinal[injecting_now == -1 & injecting_pre == -1]$rbIDU <- "Never"


# SHARING
CRISfinal$rbShareEquip <- factor(levels = c("Not sharing","Sharing", "Unknown"))
CRISfinal$rbShareEquip <- "Unknown"
CRISfinal[sharing == 1]$rbShareEquip <- "Sharing"
CRISfinal[sharing == -1]$rbShareEquip <- "Not sharing"


# HIGH RISK SEXUAL BEHAVIOR
CRISfinal$rbHRS <- factor(levels = c("No","Yes","Unknown"))
CRISfinal$rbHRS <- "Unknown"
CRISfinal[high_risk_sex == 1]$rbHRS <- "Yes"
CRISfinal[high_risk_sex == -1]$rbHRS <- "No"


# BMI
# Columns already exists, just renaming it
names(CRISfinal)[names(CRISfinal) == "weight_status"] = "phBMI" 


# HIV
CRISfinal$phHIV <- factor(levels = c("Negative","Positive","Unknown"))
CRISfinal$phHIV <- "Unknown"
CRISfinal[HIV_status == 1]$phHIV <- "Positive"
CRISfinal[HIV_status == -1]$phHIV <- "Negative"


# HBV
CRISfinal$phHBV <- factor(levels = c("Negative","Positive","Unknown"))
CRISfinal$phHBV <- "Unknown"
CRISfinal[HBV_status == 1]$phHBV <- "Positive"
CRISfinal[HBV_status == -1]$phHBV <- "Negative"


# HCV
# Adding HCV features (KConnect results)
HCVresults <- read.csv("merged_output_hepc_28d.csv")

# It is more convenient to apply the HCV classifier to a separate table
# and merge later
mod_tree <- readRDS("HCV_rf_classifier_28days.rds")
sample <- HCVresults[,c("id","positive","Negated")]
colnames(sample) <- c("BrcId","positive","Negated")
sample$pred <- predict(mod_tree, newdata=sample[,-1])

sample$positive <- NULL
sample$Negated <- NULL

# joining tables
HCVresults <- as.data.table(HCVresults)
sample <- as.data.table(sample)
setkey(HCVresults, id)
setkey(sample, BrcId)
setkey(CRISfinal, BrcId)

CRISfinal <- HCVresults[CRISfinal]
CRISfinal <- sample[CRISfinal]

CRISfinal$phHCV <- factor(levels = c("Negative","Positive","Unknown"))
CRISfinal$phHCV <- "Unknown"
CRISfinal[HCV_status == 1 | pred == 1]$phHCV <- "Positive"
CRISfinal[HCV_status == -1]$phHCV <- "Negative"



# Adding liver disease features (KConnect results)
liver_results <- as.data.table(read.csv("merged_output_liverdiseases.csv"))
liver_results <- liver_results[,c("id","positive","first_pos_date","concept")]
colnames(liver_results) <- c("BrcId","liver_positive","first_pos_date", "concept")
liver_results$first_pos_date <- as.Date(liver_results$first_pos_date, "%d/%m/%Y", tz = "Europe/London")

setkey(liver_results, BrcId)
CRISfinal <- liver_results[CRISfinal]

# Looking for diagnoses before entry (lower threshold = 0, all concepts)
CRISfinal$liver_disease_present_before <- CRISfinal$liver_positive > 0
CRISfinal$entry_date <- as.Date(CRISfinal$entry_date, "%d/%m/%Y", tz = "Europe/London")
CRISfinal$liver_disease_present_before <- (CRISfinal$first_pos_date < CRISfinal$entry_date+28 & CRISfinal$liver_disease_present_before)

# Looking for diagnoses before entry (higher threshold = 1, fewer concepts)
CRISfinal$liver_disease_present_after <- CRISfinal$liver_positive > 1
CRISfinal$liver_disease_present_after <- (CRISfinal$first_pos_date > CRISfinal$entry_date+28 & CRISfinal$liver_disease_present_after)
concepts <- c("C0023891", "C0023890", "C0023892", "C0085605", "C0020541", "C0238065")
CRISfinal[!(concept %in% concepts) & liver_disease_present_after]$liver_disease_present_after <- FALSE


write.csv(CRISfinal, "CRISfinal_2017-07-14_processed.csv")


