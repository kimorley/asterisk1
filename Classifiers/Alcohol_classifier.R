# Building and evaluating a classifier for alcohol use:
# 0 - No alcohol use
# 1 - Moderate alcohol use
# 2 - Severe alcohol use


setwd("T:/Giulia Toti/for Kate record/Alcohol classifier")
library(data.table)

cohort <- as.data.table(read.csv("Alcohol_use_sample_cleaned.csv"))
cohort <- cohort[!is.na(cohort$Label)]

cohort$Label <- as.factor(as.character(cohort$Label))

# Columns 2 to 15 are features constructed to serve the classifier. 
# Columns 16 to 35 are raw features and should not be used.

# Exploring features
summary(cohort[,c(2:15,36)])

# AAudit_risk_Cat has inconsistencies and it is better represented in
# audit_category

cohort$Audit_category <- as.factor(as.character(cohort$Audit_category))

library(caret)

featurePlot(x = cohort[,c("substanceCDAA", "substanceNDTMS", "primary_diagnosis", "secondary_diagnosis")],
            y = cohort$Label,
            plot = "box",
            auto.key=list(columns=2))

featurePlot(x = cohort[,c("substanceCDAA_level", "substanceNDTMS_level",  "AAudit_Total_Score", "Audit_category")],
            y = cohort$Label,
            plot = "pairs",
            auto.key=list(columns=2))

featurePlot(x = cohort[,c("units_of_alcohol_NDTMS", "alcohol_total_NDTMS", "Alcohol_Average", "Alcohol_Total_ID")],
            y = cohort$Label,
            plot = "pairs",
            auto.key=list(columns=2))

# One of the samples has almost no data. Eliminating.
cohort <- cohort[-4,]

# There is a strong correlation between units_of_alcohol_NDTMS and 
# Alcohol_Average.
# Both substance levels show large overlap.
# Audit_category shows good discrimination power but it is rarely full.

# Let's start with logistic regression
set.seed(123)
ctrl <- trainControl(method = "repeatedcv", number = 10, savePredictions = TRUE)
mod_fit <- train(Label ~ substanceNDTMS + primary_diagnosis +
                 units_of_alcohol_NDTMS + alcohol_total_NDTMS,
                 data=cohort, method="plr", 
                 preProcess = c("center", "scale", "medianImpute"),
                 trControl = ctrl, tuneLength = 5)

# After trying several combinations of features, we are still getting
# warnings of system computationally singular. Must change method.

# Classification tree
set.seed(123)
ctrl <- trainControl(method = "repeatedcv", number = 10, savePredictions = TRUE)
mod_tree <- train(Label ~ substanceNDTMS + primary_diagnosis +
                   units_of_alcohol_NDTMS + alcohol_total_NDTMS,
                 data=cohort, method="rpart", 
                 preProcess = c("center", "scale"),
                 trControl = ctrl, tuneLength = 5)

# This tree is using only substanceNDTMS and completely missing the 
# intermediate class (1).

# Random forest
set.seed(123)
ctrl <- trainControl(method = "repeatedcv", number = 10, savePredictions = TRUE)
mod_tree <- train(Label ~ substanceNDTMS + primary_diagnosis +
                    units_of_alcohol_NDTMS + alcohol_total_NDTMS,
                  data=cohort, method="rf", 
                  preProcess = c("center", "scale"),
                  trControl = ctrl, tuneLength = 5)

varImp(mod_tree)
pred = predict(mod_tree, newdata=cohort)
confusionMatrix(data=pred, cohort$Label)

saveRDS(mod_tree, "Alcohol_rf_classifier.rds")

# SVM (radial)
set.seed(123)
ctrl <- trainControl(method = "repeatedcv", number = 10, savePredictions = TRUE)
mod_svm <- train(Label ~ substanceNDTMS + primary_diagnosis +
                    units_of_alcohol_NDTMS + alcohol_total_NDTMS,
                  data=cohort, method="svmRadial", 
                  preProcess = c("center", "scale"),
                  trControl = ctrl, tuneLength = 5)


# SVM (poly)
set.seed(123)
ctrl <- trainControl(method = "repeatedcv", number = 10, savePredictions = TRUE)
mod_svm <- train(Label ~ substanceNDTMS + primary_diagnosis +
                   units_of_alcohol_NDTMS + alcohol_total_NDTMS,
                 data=cohort, method="svmPoly", 
                 preProcess = c("center", "scale"),
                 trControl = ctrl, tuneLength = 5)

pred = predict(mod_svm, newdata=cohort)
confusionMatrix(data=pred, cohort$Label)

saveRDS(mod_svm, "Alcohol_svm_classifier.rds")
