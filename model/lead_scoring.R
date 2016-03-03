# preliminaries
library(e1071)
library(pmml)
library(caret)
library(doMC)
library(ROCR)
library(glmnet)
library(boot)
library(rpart)
library(randomForest)

# increase cores
registerDoMC(cores = 6)

# read in data
df <- read.csv(file = "~/Downloads/leads_to_train.csv", header = TRUE, sep = ",", check.names = FALSE, stringsAsFactors = TRUE)

# clean column names, populate NAs where appropriate, transform booelans to 0-1 factors
names(df) <- tolower(gsub(' ', '_' , gsub('Lead Scoring ', '' , names(df))))

# break out scoring meetings versus opportunities
opportunities <- df[, 3:ncol(df)]
meetings <- df[, c(2, 4:ncol(df))]

# train-test split function
train_test_split <- function(data, train_pct = 0.6, seed = NULL) {
	if (!is.null(seed)) set.seed(seed)
	sample_size <- floor(train_pct * nrow(data))
	index <- sample(seq_len(nrow(data)), size = sample_size)
	train_set <- data[index, ]
	test_set <- data[-index, ]
	list(train_set = train_set, test_set = test_set)
}

# split data into training and test sets
splits <- train_test_split(meetings, train_pct = 0.8, seed = 111)

# grab training and test sets
train <- splits[[1]]
test <- splits[[2]]

# create matrix of factors, remove near-zero-variance variables, elimination perfectly co-linear variables #
X <- model.matrix(~., data = train[,-1])[,-1]
# y <- train[rownames(X), 1]   # y to accompany X
nzv <- nearZeroVar(X)
X.new <- X[, -nzv]
corr_mat <- cor(X.new)
too_high  <- findCorrelation(corr_mat, cutoff = 0.9)
if(length(too_high) == 0){
  XX <- X.new
  } else {
  XX <- X.new[, -too_high]
}
y <- train[rownames(XX), 1]


#
# naïve bayes
#

keep_cols <- c("meeting", "founder", "ceo", "cto", "vp", "director", "manager", "engineer_developer", "analyst", "product", "department", "inbound", "original_referrer", "campaign_touches", "first_campaign", "nth_contact", "company_type", "number_of_employees", "state", "country")
train.sub <- train[, keep_cols]
nb.model <- naiveBayes(as.factor(meeting) ~ ., data = train.sub)
predicted_values <- predict(nb.model, test[, keep_cols[-1]])
confusionMatrix(predicted_values, test$meeting)
# saveXML(pmml.naiveBayes(nb.model, predictedField = "meeting"), "/Users/scott/Documents/applications/openscoring-heroku/pmml/BayesLeadScore.pmml")

# for later AUC visualization #
predicted_values.1 <- predict(nb.model, na.omit(test[, keep_cols[-1]]), type = "raw")
pred.1 <- prediction(predicted_values.1[,2], na.omit(test[, keep_cols])[,1])
perf.1 <- performance(pred.1, "tpr", "fpr")


#
# logistic regression with elastic-net penalty
#

enet.model <- cv.glmnet(y ~ X, family = "binomial", alpha = seq(0.2,0.5,0.8), nfolds = 5, parallel = TRUE)
Z <- model.matrix(~., data = test[,-1])[,-1]
y <- test[rownames(Z),1]
predicted_values <- inv.logit(predict(enet.model, Z, s = min(enet.model$lambda.1se)))
confusionMatrix(ifelse(predicted_values > 0.5, 1, 0), y)
# saveXML(pmml.naiveBayes(enet.model, predictedField = "meeting"), "/Users/scott/Documents/applications/openscoring-heroku/pmml/LogisticLeadScore.pmml")

# for later AUC visualization #
pred.2 <- prediction(predicted_values, y)
perf.2 <- performance(pred.2, "tpr", "fpr")


#
# decision tree
#

dt.model <- rpart(meeting ~ ., data = train, method = "class")
# plotcp(dt.model)  # use this to get cutoff
pruned.dt.model <- prune(dt.model, 0.015)
predicted_values <- predict(pruned.dt.model, na.omit(test)[,-1])
confusionMatrix(ifelse(predicted_values[,2] > 0.5, 1, 0), na.omit(test)[,1])

# for later AUC visualization #
pred.3 <- prediction(predicted_values[,2], na.omit(test)[,1])
perf.3 <- performance(pred.3, "tpr", "fpr")


#
# support vector classifier
#

svc.model <- svm(y ~ XX, data = NULL)
XX <- model.matrix(~., data = test[, keep_cols])[,-1]
y <- test[rownames(XX), 1]
predicted_values <- predict(svc.model, XX)
confusionMatrix(ifelse(predicted_values > 0.5, 1, 0), y)

# for later AUC visualization #
pred.4 <- prediction(predicted_values, y)
perf.4 <- performance(pred.5, "tpr", "fpr")


#
# build the AUC visualization #
#

legend <- c("Naive Bayes, AUC =", "Elastic-Net Logistic, AUC =", "Decision Tree, AUC =", "SVC =")
legend[1] <- paste(legend[1], round((performance(pred.1, 'auc')@y.values)[[1]],3))
legend[2] <- paste(legend[2], round((performance(pred.2, 'auc')@y.values)[[1]],3))
legend[3] <- paste(legend[3], round((performance(pred.3, 'auc')@y.values)[[1]],3))
legend[4] <- paste(legend[4], round((performance(pred.4, 'auc')@y.values)[[1]],3))
plot(perf.1, col = "blue", lwd = 2)
plot(perf.2, col = "red", lwd = 2, add = TRUE)
plot(perf.3, col = "green", lwd = 2, add = TRUE)
plot(perf.4, col = "black", lwd = 2, add = TRUE)
abline(0, 1, col = "grey")
legend(0.45, 0.6, legend, lty=1, lwd=2, col=c("blue", "red", "green", "black"))


#
# correlation matrix visualization
#

to_extract <- regexec('[0-9A-Z].*', colnames(X.new))
foo <- c()
for (i in 1:length(to_extract)){
  if(to_extract[[i]][1] != -1){
    foo[i] <- paste(substr(colnames(X.new)[i], 0, to_extract[[i]][1] - 1), '-', print(substr(colnames(X.new)[i], to_extract[[i]][1], nchar(colnames(X.new)[i]))))
  } else {
    foo[i] <- colnames(X.new)[i]
  }
}

foo <- gsub('department', 'dept - ', foo)
foo <- gsub('number_of_employees', 'employees', foo)
foo <- gsub('state', 'state - ', foo)
foo <- gsub('country', 'country - ', foo)
foo[21] <- gsub('Acq_', '', gsub('(Outbound_Research).*', '\\1', foo[21]))
colnames(X.new) <- foo

cor.mtest <- function(mat, ...) {
    mat <- as.matrix(mat)
    n <- ncol(mat)
    p.mat<- matrix(NA, n, n)
    diag(p.mat) <- 0
    for (i in 1:(n - 1)) {
        for (j in (i + 1):n) {
            tmp <- cor.test(mat[, i], mat[, j], ...)
            p.mat[i, j] <- p.mat[j, i] <- tmp$p.value
        }
    }
  colnames(p.mat) <- rownames(p.mat) <- colnames(mat)
  p.mat
}

corrplot(corr_mat, type="upper", order="hclust", p.mat = p.mat, sig.level = 0.01, insig = "blank")
