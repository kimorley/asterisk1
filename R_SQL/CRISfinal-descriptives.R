# basic descriptives on CRIS data only
# kimorley - 27/7/17

require(data.table)
require(car)
require(ggplot2)

setwd('T:/Giulia Toti/ASTERISK cohort')
source('T:/Giulia Toti/ASTERISK cohort/freqTable.R')

#------------------------------------------------

# LOAD FILE #####

load('CRISfinal_2017-07-14_processed.RData')

# PRELIMINARY CLEANING #####

# create table for patient numbers

sampleSize <- data.frame(step='raw', size=nrow(CRISfinal))

# remove patients who did not attend clinics

CRISfinal <- CRISfinal[ !(clinicLocation %in% c('REMOVE'))]

sampleSize <- rbind(sampleSize, cbind(step='wrongClinic', size=nrow(CRISfinal)))

# remove those who had liver disease beforehand

CRISfinal <- CRISfinal[ !(liver_disease_present_before %in% c('TRUE'))]

sampleSize <- rbind(sampleSize, cbind(step='ldBefore', size=nrow(CRISfinal)))


# SOCIODEMOGRAPHICS #####

# clinic, age, gender, ethnicity, living situation, employment

freqTable(CRISfinal, vars=c('clinicLocation','Gender_ID','ethnicity',
                            'livingSituation','employmentStatus'), 
          varNames=c('Clinic location','Gender','Ethnicity',
                     'Housing status','Employment'),
          returnTable=T, latex=F, excel=T, fileName='socioDemo')

# SUBSTANCE USE ####

# (alcohol to come), opiate use, methadone/buprenorphine, cocaine, benzo, cannabis

freqTable(CRISfinal, vars=c('opiateUse','mbUse','cocaineUse',
                            'benzoUse','cannUse'), 
          varNames=c('Opiates','Methadone or Buprenorphine','Cocaine/Crack',
                     'Benzodiazepines','Cannabis (inc skunk)'),
          returnTable=T, latex=F, excel=T, fileName='drugUse')

# MENTAL HEALTH #####

# psychosis, anxiety/depression, mania

freqTable(CRISfinal, vars=grep('mh',names(CRISfinal), value=TRUE), 
          varNames=c('Psychosis','Anxiety/depression','Mania'),
          returnTable=T, latex=F, excel=T, fileName='mentalHealth')

# PHYSICAL HEALTH ####

freqTable(CRISfinal, vars=grep('ph',names(CRISfinal), value=TRUE)[3:6], 
          varNames=c('BMI','HIV status','HBV status','HCV status'),
          returnTable=T, latex=F, excel=T, fileName='physicalHealth')

# RISK BEHAVIOURS ####

freqTable(CRISfinal, vars=grep('rb',names(CRISfinal), value=TRUE), 
          varNames=c('Injecting status','Sharing equipment','High-risk sexual behaviour'),
          returnTable=T, latex=F, excel=T, fileName='riskBehaviour')
