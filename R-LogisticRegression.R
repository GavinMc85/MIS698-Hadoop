setwd("C:/Users/gmm372/Dropbox/School/MIS 698 (Hadoop)/Final Project/")


hadoop <- read.csv(file="trainfeatures.csv")

detach("package:dplyr", unload=TRUE)
library("plyr")
hadoop <- rename(hadoop, c("X0" = "booking_bool", "X5" = "srch_booking_window", "X0.1" = "srch_children_count", "X1" = "srch_length_of_stay", 
                 "X.N" = "comp3_inv", "X1.1" = "srch_room_count", "X.N.1" = "comp1_rate", "X.N.2" = "comp2_rate", "X.N.3" = "comp3_rate",
                 "X.N.4" = "comp4_rate", "X.N.5" = "comp5_rate", "X.N.6" = "comp6_rate", "X.N.7" = "comp7_rate", "X.N.8" = "comp8_rate",
                 "X0.2" = "prop_starrating", "X2.0" = "prop_review_score", "X.N.9" = "srch_query_affinity_score", "X0.3" = "promotional_flag", 
                 "X.N.10" = "prop_location_score2"))


hadoop[hadoop=="\\N"]<-NA
hadoop$prop_review_score <- as.numeric(as.character(hadoop$prop_review_score))
hadoop$srch_query_affinity_score <- as.numeric(as.character(hadoop$srch_query_affinity_score))
hadoop$prop_location_score2 <- as.numeric(as.character(hadoop$prop_location_score2))

library(dplyr)
hadoop <- hadoop %>% mutate(prop_location_score2 = ifelse(is.na(prop_location_score2),0.1308,prop_location_score2))
hadoop <- hadoop %>% mutate(prop_review_score = ifelse(is.na(prop_review_score), 3.8, prop_review_score))

hadoop2 <- hadoop[,c(1, 2, 3, 4, 6, 15, 16, 18, 19)]

set.seed(31)
nall <- 6977877
ntrain <- floor(0.8*nall)
ntest <- floor(0.2*nall)

index <- seq(1:nall)
train <- sample(index,ntrain)
newindex <- index[-train]
test <- sample(newindex,ntest)

#validate that it worked
length(train)
length(test)
length(train) + length(test) 

library(caret)
library(pROC)

fit <- glm(booking_bool ~ ., data=hadoop2[train,], family=binomial())
stepwise <- step(fit)

summary(fit)
varImp(fit, scale=FALSE)


#tried to use this to plot ROC curve - ran for ~3 hours, gave up
prop <- predict(fit, type=c("response"))
hadoop2[train,]$prop <- prop
g <- roc(booking_bool ~ prop, data=hadoop2[train,])
plot(g)

#fit predictions
fit.predict <- ifelse(predict(fit, hadoop2[test,], type="response") > 0.5, TRUE, FALSE)
y <- hadoop2[test,]
confusion <- table(fit.predict, as.logical(y$booking_bool))

#this next line comes up as an error since predict function predicts everything to be false
confusion  <- cbind(confusion, c(1 - confusion[1,1]/(confusion[1,1]+confusion[2,1]), 1 - confusion[2,2]/(confusion[2,2]+confusion[1,2])))
confusion  <- as.data.frame(confusion)
names(confusion) <- c('FALSE', 'TRUE', 'class.error')
confusion
log1error=(confusion[1,2]+confusion[2,1])/1395575
log1error

#With a low rate of positive bookings, decided to try penalized regression using GLMnet package


library(glmnet)

#glmnet only works on matrices - need to transform using model.matrix

rawdatamatrix <- model.matrix(booking_bool ~ srch_booking_window + srch_children_count + srch_room_count + prop_starrating + prop_review_score + 
                                promotional_flag + prop_location_score2, data=hadoop2)

#drop intercept from model matrix, and define x and y variables
x <- rawdatamatrix[,-1]
x <- x[train,]
y <- hadoop2[train,]

fit1 <-glmnet(x, y$booking_bool, family="binomial")
print(fit1)
plot(fit1)
coef(fit1, s=0.01)

x <- rawdatamatrix[,-1]
x <- x[test,]
y <- hadoop2[test,]

log1pred <- predict(fit1, x, s=0.005, type="class")

logtable <- table(log1pred, y$booking_bool)
logtable

#once again we see that the regression predicts all negatives
log1error=(logtable[1,2]+logtable[2,1])/1395575
log1error

