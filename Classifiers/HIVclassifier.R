# Building and evaluating a patient level HIV classifier

setwd("T:/Giulia Toti/for Kate record/HIVclassifier")
library(data.table)

##############
### PART 1 ###
##############

# Creation and labeling of a sample of patients from the
# addiction cohort

# Dataset: cohort of 1000 patients of unknown HIV status,
# selected from info extracted on May 23 2017 
cohort <- as.data.table(read.csv("comorbiditiesRSQL_2017-05-23.csv"))
set.seed(4)
cohort <- cohort[HIV_status == 0]
cohort <- cohort[sample(1:nrow(cohort),1000),c("BrcId","entry_date"),with=FALSE]
setkey(cohort, BrcId)


# Some patients are no longer included in the cohort, 
# probably because they come from ward stay. For consistency, 
# we should not use them for training
complete <- read.csv("T:/Giulia Toti/Honghan/complete_addiction_cohort_entry_date_July10.csv")
include <- setdiff(cohort$BrcId,setdiff(cohort$BrcId,complete$BrcId))
cohort <- cohort[BrcId %in% include]

# Query for HIV app:
# select gh.BrcId, gh.CN_Doc_ID, gh.numWords, gh.contextString, 
# gh.mlObservation1, gh.MLpriority, gh.prob, vw.Date
# from gate_hunter_hiv306 gh
# join 
# vw_gate vw
# on gh.CN_Doc_ID = vw.CN_Doc_ID
app_results <- as.data.table(read.csv("HIV306app_results.csv"))
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


# Query for HIV treatment app:
# select gh.BrcId, gh.CN_Doc_ID, gh.match, gh.numWords, gh.contextString, 
# gh.mlObservation1, gh.MLpriority, gh.prob, vw.Date
# from gate_hunter_hivtreatment306 gh 
# join 
# vw_gate vw
# on gh.CN_Doc_ID = vw.CN_Doc_ID

app_treatment <- as.data.table(read.csv("HIV306app_results_treatment.csv", stringsAsFactors = FALSE))
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
                                    total_positive_treat = sum(mlObservation1 == "positive"),
                                    total_negative_treat = sum(mlObservation1 == "negative"),
                                    avg_prob_treat = mean(prob),
                                    avg_MLpriority_treat = mean(MLpriority)),
                             by = BrcId]

# Adding pos/neg ratio
sample_treat$pn_ratio_treat <- sample_treat$total_positive_treat/sample_treat$total_negative_treat
sample_treat[pn_ratio_treat == Inf]$pn_ratio_treat <- 100 #caret does not handle Inf

# Filling in some NAs
sample_treat$total_positive_treat[sample_treat$total_mentions_treat==0] <- 0
sample_treat$total_negative_treat[sample_treat$total_mentions_treat==0] <- 0
sample_treat$avg_prob_treat[sample_treat$total_mentions_treat==0] <- 0
sample_treat$avg_MLpriority_treat[sample_treat$total_mentions_treat==0] <- 0
sample_treat$pn_ratio_treat[sample_treat$total_mentions_treat==0] <- 0

# joining the two final features tables 
sample <- sample_treat[sample]


# Adding labels to patients identified as HIV positive 
# (from notes)
sample$HIV_status <- factor("negative", levels = c("positive", "negative"))
sample[BrcId == 10000745]$HIV_status <- "positive"
# sample[BrcId == 10046441]$HIV_status <- "positive"
sample[BrcId == 10047442]$HIV_status <- "positive"
sample[BrcId == 10048865]$HIV_status <- "positive"
sample[BrcId == 10053605]$HIV_status <- "positive"
sample[BrcId == 10054197]$HIV_status <- "positive"
# sample[BrcId == 10056778]$HIV_status <- "positive"
# sample[BrcId == 10057306]$HIV_status <- "positive"
sample[BrcId == 10064071]$HIV_status <- "positive"
sample[BrcId == 10066588]$HIV_status <- "positive"
sample[BrcId == 10080773]$HIV_status <- "positive"
sample[BrcId == 10094380]$HIV_status <- "positive"
sample[BrcId == 10095146]$HIV_status <- "positive"
sample[BrcId == 10097096]$HIV_status <- "positive"
# sample[BrcId == 10139714]$HIV_status <- "positive"
sample[BrcId == 10172506]$HIV_status <- "positive"
# sample[BrcId == 10174537]$HIV_status <- "positive"
sample[BrcId == 10182793]$HIV_status <- "positive"
sample[BrcId == 10217290]$HIV_status <- "positive"
sample[BrcId == 10248500]$HIV_status <- "positive"
sample[BrcId == 10256486]$HIV_status <- "positive"

# Some variables have 0 variance, or are duplicates,
# we can eliminate them
sample$total_negative_treat <- NULL
sample$total_positive_treat <- NULL
sample$pn_ratio_treat <- NULL

library(caret)

featurePlot(x = sample[,c("total_mentions","total_positive","total_negative")],
            y = sample$HIV_status,
            plot = "pairs",
            auto.key=list(columns=2))

featurePlot(x = sample[,c("avg_prob","avg_MLpriority","pn_ratio")],
            y = sample$HIV_status,
            plot = "pairs")

featurePlot(x = sample[,c("total_mentions_treat","avg_prob_treat","avg_MLpriority_treat")],
            y = sample$HIV_status,
            plot = "pairs")


# There does not seem to be visible difference between negative
# and unknown subjects, so we are going to group them in one class

# Some variables have a small variability, it makes sense to 
# bin them
sample$avg_prob_treat <- sample$avg_prob_treat > 0
# sample$pn_ratio_treat <- sample$pn_ratio_treat > 50
sample$pn_ratio <- sample$pn_ratio > 50


# We identified an outlier and we are removing it:
sample <- sample[BrcId != 10248500] 



##############
### PART 2 ###
##############

# Training a classifier

# We are going to start evaluating the efficacy of a very simple
# assumption: positive if total_positive > 0

preds <- factor(levels = c("positive", "negative"))
preds[1:nrow(sample)] <- "negative"
preds[sample$total_positive > 0] <- as.factor("positive")
confusionMatrix(data=preds, sample$HIV_status, positive = "positive")

# Trying now avg_prob > 0
preds <- factor(levels = c("positive", "negative"))
preds[1:nrow(sample)] <- "negative"
preds[sample$avg_prob > 0] <- as.factor("positive")
confusionMatrix(data=preds, sample$HIV_status, positive = "positive")

# total_mentions_treat > 0
preds <- factor(levels = c("positive", "negative"))
preds[1:nrow(sample)] <- "negative"
preds[sample$total_mentions_treat > 0] <- as.factor("positive")
confusionMatrix(data=preds, sample$HIV_status, positive = "positive")

# avg_prob_treat > 0
preds <- factor(levels = c("positive", "negative"))
preds[1:nrow(sample)] <- "negative"
preds[sample$avg_prob_treat == TRUE] <- as.factor("positive")
confusionMatrix(data=preds, sample$HIV_status, positive = "positive")


# This simple classifiers tend to produce a high amount of false 
# positive, which we want to avoid

# Let's try logistic regression
set.seed(123)
ctrl <- trainControl(method = "repeatedcv", number = 10, savePredictions = TRUE)
mod_fit <- train(HIV_status ~ . - BrcId,
                 data=sample, method="plr", 
                 preProcess = c("center", "scale"),
                 trControl = ctrl, tuneLength = 5)

exp(coef(mod_fit$finalModel))

pred = predict(mod_fit, newdata=sample)
confusionMatrix(data=pred, sample$HIV_status, positive = "positive")


# Modeling with trees
set.seed(123)
Train <- createDataPartition(sample$HIV_status, p=0.75, list=FALSE)
training <- sample[ Train, ]
testing <- sample[ -Train, ]

ctrl <- trainControl(method = "repeatedcv", number = 10, savePredictions = TRUE)

mod_tree <- train(HIV_status ~ . - BrcId,
                  data=sample, method="rf", 
                  preProcess = c("center", "scale"),
                  trControl = ctrl, tuneLength = 5)

varImp(mod_tree)

plot(mod_tree$finalModel, uniform = TRUE)
text(mod_tree$finalModel, use.n = TRUE, all = TRUE, cex = .8)

pred = predict(mod_tree, newdata=sample)
confusionMatrix(data=pred, sample$HIV_status, positive = "positive")

confusionMatrix(mod_tree$pred$pred, mod_tree$pred$obs, positive = "positive")

saveRDS(mod_tree, "HIV_rf_classifier.rds")

# Let's try logistic regression with the only variable selected
# by CART
set.seed(123)
ctrl <- trainControl(method = "repeatedcv", number = 10, savePredictions = TRUE)
mod_fit <- train(HIV_status ~ total_mentions_treat,
                 data=sample, method="plr", 
                 preProcess = c("center", "scale"),
                 trControl = ctrl, tuneLength = 5)

