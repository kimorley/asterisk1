# Building and evaluating a patient level HIV classifier

setwd("T:/Giulia Toti/HCV classifier")
library(data.table)

##############
### PART 1 ###
##############

# Importing and cleaning features table

sample <- as.data.table(read.csv("hepc_1k_features_28days.csv"))

# Some patients are no longer included in the cohort, 
# probably because they come from ward stay. For consistency, 
# we should not use them for training
complete <- read.csv("complete_cohort_CRISfinal.csv")
include <- setdiff(sample$id,setdiff(sample$id,complete$BrcId))
sample <- sample[id %in% include]

summary(sample)

# Most of the drugs columns are empty, so we can eliminate and keep 
# only Tot_drugs

sample$TELAPREVIR <- NULL
sample$RIBAVIRIN <- NULL
sample$SOFOSBUVIR <- NULL
sample$RITONAVIR <- NULL
sample$DACLATASVIR <- NULL
sample$PEGINTERFERON_ALPHA <- NULL
sample$BOCEPREVIR <- NULL
sample$LEDIPASVIR <- NULL

# Label needs to be a factor
sample[HCV_status == -1] <- 0
sample$HCV_status <- as.factor(sample$HCV_status)


# Exploring feature distribution
library(caret)

featurePlot(x = sample[,c("all","positive","Negated")],
            y = sample$HCV_status,
            plot = "pairs",
            auto.key=list(columns=2))

featurePlot(x = sample[,c("hypothetical","historical","Other")],
            y = sample$HCV_status,
            plot = "pairs",
            auto.key=list(columns=2))

# hypothetical, hystorical, Tot_drugs and other are scarcely 
# populated and probably not very helpful...


##############
### PART 2 ###
##############

# Training a classifier

# We are going to start evaluating the efficacy of a very simple
# assumption: positive if total_positive > 0

preds <- factor(levels = c(0, 1))
preds[1:nrow(sample)] <- as.factor(0)
preds[sample$positive >= 1] <- as.factor(1)
confusionMatrix(data=preds, sample$HCV_status)

# This classifier captures 15/16 positives but produces 33 false
# positives.

set.seed(123)
ctrl <- trainControl(method = "repeatedcv", number = 10, savePredictions = TRUE)
mod_fit <- train(HCV_status ~ positive + Negated,
                 data=sample, method="plr", 
                 preProcess = c("center", "scale"),
                 trControl = ctrl, tuneLength = 5)


# Modeling with trees
set.seed(123)

ctrl <- trainControl(method = "repeatedcv", number = 10, savePredictions = TRUE)

mod_tree <- train(HCV_status ~ positive + Negated,
                  data=sample, method="rf", 
                  preProcess = c("center", "scale"),
                  trControl = ctrl, tuneLength = 5)


varImp(mod_tree)

pred = predict(mod_tree, newdata=sample)
confusionMatrix(data=pred, sample$HCV_status, positive = "1")

saveRDS(mod_tree, "HCV_rf_classifier_28days.rds")

