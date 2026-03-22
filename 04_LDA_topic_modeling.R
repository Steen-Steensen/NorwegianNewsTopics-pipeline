################################################################################
# LATENT DIRICHLET ALLOCATION (LDA) TOPIC MODELING WITH MALLET
################################################################################
#
# PURPOSE:
#   Trains an LDA topic model using MALLET, evaluates models with different
#   numbers of topics (k), determines optimal k, and produces document-topic
#   distributions for both training and test data.
#
# INPUT:
#   - tokens_list.rds: R list object from 03_create_DTM.R (one token vector per document)
#   - alltexts_preprocessed.rds: Data frame with document metadata and text (Nynorsk translated to Bokmål)
#
# OUTPUT:
#   - merged-doc-topics-28.txt: Combined document-topic distribution matrix
#   - topic-keys-28.txt: Topic word distributions
#   - doc-topics-28.txt: Training document-topic matrix
#   - alltexts.rds: Updated metadata file (filtered documents)
#   - lda_evaluate5.csv: Model evaluation metrics
#
# PROCESSING STEPS:
#   1. Filter low/high frequency tokens
#   2. Create MALLET-format corpus files (train/test)
#   3. Train LDA models with different k values
#   4. Evaluate models using log-likelihood
#   5. Determine optimal k using segmented regression AND RMSE minimization
#   6. Train final model with k=28 and generate predictions
#
# ============================================================================
# SYSTEM REQUIREMENTS
# ============================================================================
#
# This script requires both a Java JDK and the MALLET toolkit to be installed
# and accessible on the system PATH before running.
#
# 1. JAVA JDK
#    The rJava and mallet R packages require a full Java Development Kit (JDK),
#    not just a JRE. The recommended distribution is Eclipse Temurin (OpenJDK):
#
#      brew install --cask temurin
#
#    After installing, reconfigure R's Java settings (run in Terminal, not R):
#
#      export JAVA_HOME=$(/usr/libexec/java_home)
#      sudo R CMD javareconf
#
#    Then reinstall rJava inside R:
#
#      install.packages("rJava")
#
# 2. MALLET
#    MALLET is a Java-based NLP toolkit used here for LDA topic modeling.
#    Install via Homebrew:
#
#      brew install mallet
#
#    Verify the installation:
#
#      mallet import-file --help
#
#    If the brew formula is unavailable, download MALLET manually from:
#      http://mallet.cs.umass.edu/download.php
#    Extract it (e.g. to ~/mallet) and add the bin directory to your PATH:
#
#      echo 'export PATH=$PATH:~/mallet/bin' >> ~/.zshrc
#      source ~/.zshrc
#
#    NOTE: Start a new R session after installing MALLET so that R inherits
#    the updated PATH. The options(java.parameters = ...) line below must also
#    be executed before library(mallet) in the same session.
#
################################################################################

# Set Java memory limit to 8GB
# NOTE: this MUST come before library(mallet) / any rJava call or it has no effect
options(java.parameters = "-Xmx8g")

# Load required libraries
library(dplyr)
library(purrr)
library(mallet)
library(readr)
library(data.table)
library(parallel)
library(tidyverse)
library(ggplot2)
library(segmented)

################################################################################
# LOAD INPUTS
################################################################################

cat("\nLoading tokens_list from 03_create_DTM.R output...\n")
tokens_list <- readRDS("tokens_list.rds")

cat("Loading alltexts from 02_translate_nynorsk_clean.R output...\n")
alltexts <- readRDS("alltexts_preprocessed.rds")

################################################################################
# SECTION 1: FILTER LOW/HIGH FREQUENCY TOKENS
################################################################################

cat("\nFiltering very rare and very frequent tokens...\n")

# Define filtering thresholds
high_freq_threshold <- 0.9   # Remove tokens appearing in >90% of documents
low_doc_threshold <- 10      # Remove tokens appearing in <=10 documents

# Convert tokens_list to data.table for efficient processing
tokens_dt <- rbindlist(lapply(seq_along(tokens_list), function(i) {
  data.table(doc_id = i, token = tokens_list[[i]])
}), use.names = TRUE)

# Detect available cores for parallel processing
cores <- detectCores() - 1
cl <- makeCluster(cores)

# Export variables to cluster nodes
clusterExport(cl, varlist = c("tokens_dt", "high_freq_threshold", "low_doc_threshold", "data.table"))

# Function to filter tokens in parallel chunks
filter_tokens_parallel <- function(dt_chunk) {
  library(data.table)
  total_docs <- uniqueN(dt_chunk$doc_id)
  doc_freq <- dt_chunk[, .N, by = .(token)]
  tokens_to_remove <- doc_freq[(N > high_freq_threshold * total_docs) | (N <= low_doc_threshold), token]
  filtered_chunk <- dt_chunk[!token %in% tokens_to_remove]
  return(filtered_chunk)
}

# Split into chunks and apply parallel filtering
chunk_size <- ceiling(nrow(tokens_dt) / cores)
data_chunks <- split(tokens_dt, rep(1:cores, each = chunk_size, length.out = nrow(tokens_dt)))
filtered_chunks <- parLapply(cl, data_chunks, filter_tokens_parallel)

stopCluster(cl)

# Recombine filtered data
filtered_tokens_dt <- rbindlist(filtered_chunks)
filtered_tokens_list <- split(filtered_tokens_dt$token, filtered_tokens_dt$doc_id)

saveRDS(filtered_tokens_list, "filtered_tokens_list.rds")

cat("Filtering complete. Tokens reduced from", length(tokens_list), "to", length(filtered_tokens_list), "documents.\n")

gc()

################################################################################
# SECTION 2: CREATE MALLET-FORMAT CORPUS FILE
################################################################################

cat("\nCreating corpus file in MALLET format...\n")

doc_ids <- names(filtered_tokens_list)
out_file <- "my_corpus.txt"
file.create(out_file)

# Write corpus file in chunks for memory efficiency
chunk_size <- 5000
n_docs <- length(filtered_tokens_list)

pb <- txtProgressBar(min = 0, max = n_docs, style = 3)

for (start_idx in seq(1, n_docs, by = chunk_size)) {
  end_idx <- min(start_idx + chunk_size - 1, n_docs)
  chunk <- filtered_tokens_list[start_idx:end_idx]
  chunk_ids <- doc_ids[start_idx:end_idx]
  chunk_texts <- sapply(chunk, function(tokvec) paste(tokvec, collapse = " "))
  chunk_lines <- paste(chunk_ids, chunk_texts)
  cat(chunk_lines, file = out_file, sep = "\n", append = TRUE)
  setTxtProgressBar(pb, end_idx)
}

close(pb)

cat("Corpus file created with", n_docs, "documents.\n")


################################################################################
# SECTION 3: LOAD AND CLEAN CORPUS DATA
################################################################################

cat("\nLoading corpus from file...\n")

# Read corpus file and extract document IDs and text
lines <- read_lines("my_corpus.txt")

corpus_df <- tibble(
  doc_id = sub(" .*", "", lines),
  text = sub("^[^ ]+ ", "", lines)
)

cat("Successfully loaded corpus with", nrow(corpus_df), "documents.\n")

setDT(alltexts)
alltexts <- alltexts[, !grepl("^Topic", names(alltexts)), with = FALSE]
saveRDS(alltexts, "alltexts.rds")

# Extract document IDs and text for train/test split
doc_ids <- corpus_df$doc_id
docs <- corpus_df$text

rm(corpus_df)
gc()

################################################################################
# SECTION 4: CREATE TRAIN/TEST SPLIT
################################################################################

cat("\nCreating train/test split (80/20)...\n")

set.seed(42)
n_docs <- length(docs)

train_indices <- sample(seq_len(n_docs), size = 0.8 * n_docs)
test_indices <- setdiff(seq_len(n_docs), train_indices)

train_ids <- doc_ids[train_indices]
train_docs <- docs[train_indices]
test_ids <- doc_ids[test_indices]
test_docs <- docs[test_indices]

# Remove specific word from all documents
word_to_remove <- "sier"
train_docs <- gsub(paste0("\\b", word_to_remove, "\\b"), "", train_docs)
test_docs <- gsub(paste0("\\b", word_to_remove, "\\b"), "", test_docs)

# Clean up extra whitespace
train_docs <- gsub("\\s+", " ", trimws(train_docs))
test_docs <- gsub("\\s+", " ", trimws(test_docs))

cat("Successfully removed word:", word_to_remove, "\n")
cat("Train set size:", length(train_docs), "\n")
cat("Test set size:", length(test_docs), "\n")



################################################################################
# SECTION 5: WRITE TRAIN/TEST FILES IN MALLET FORMAT
################################################################################

cat("\nWriting train and test files...\n")

train_file <- "train.txt"
test_file <- "test.txt"
dummy_label <- "X"

# Create MALLET-compatible format: doc_id label text
train_lines <- paste(train_ids, dummy_label, train_docs)
test_lines <- paste(test_ids, dummy_label, test_docs)

# Write with UTF-8 encoding
writeLines(enc2utf8(train_lines), train_file, useBytes = TRUE)
writeLines(enc2utf8(test_lines), test_file, useBytes = TRUE)

cat("Train and test files created.\n")

gc()

################################################################################
# SECTION 6: IMPORT DATA INTO MALLET FORMAT
################################################################################

cat("\nImporting data into MALLET format...\n")

train_mallet_file <- "train.mallet"
test_mallet_file <- "test.mallet"

# Set Java memory options
Sys.setenv("_JAVA_OPTIONS" = "-Xmx8g -XX:-UseGCOverheadLimit")

# Import training data
cmd_train_import <- sprintf(
  "mallet import-file --input %s --output %s --line-regex '^(\\S+)\\s+(\\S+)\\s+(.*)$' --name 1 --label 2 --data 3 --keep-sequence TRUE --remove-stopwords FALSE --encoding UTF-8",
  train_file, train_mallet_file
)
system(cmd_train_import)

# Import test data using training pipe
cmd_test_import <- sprintf(
  "mallet import-file --input %s --output %s --use-pipe-from %s --encoding UTF-8",
  test_file, test_mallet_file, train_mallet_file
)
system(cmd_test_import)

cat("Data successfully converted to MALLET format.\n")

################################################################################
# SECTION 7: FUNCTION TO TRAIN AND EVALUATE LDA MODELS
################################################################################

# Function to train model with k topics and evaluate on test data
train_and_eval <- function(k, iterations = 1000) {
  # Define output file paths
  state_file <- sprintf("topic-state-%d.txt", k)
  topic_keys_file <- sprintf("topic-keys-%d.txt", k)
  doc_topics_file <- sprintf("doc-topics-%d.txt", k)
  doc_probs_file <- sprintf("doc-probs-%d.txt", k)
  evaluator_file <- sprintf("topic-evaluator-%d", k)

  # Train LDA model
  train_cmd <- sprintf(
    "mallet train-topics --input %s --num-topics %d --num-iterations %d --output-state %s --output-topic-keys %s --output-doc-topics %s --evaluator-filename %s",
    train_mallet_file, k, iterations, state_file, topic_keys_file, doc_topics_file, evaluator_file
  )

  cat("\nTraining model with", k, "topics...\n")
  system(train_cmd, intern = TRUE)

  # Check if evaluator file was created
  if (!file.exists(evaluator_file)) {
    cat("Error: Evaluator file was not created for k =", k, "\n")
    return(NA_real_)
  }

  # Evaluate model on test data
  eval_cmd <- sprintf(
    "mallet evaluate-topics --input %s --evaluator %s --output-doc-probs %s",
    test_mallet_file, evaluator_file, doc_probs_file
  )

  cat("Evaluating model with", k, "topics...\n")
  eval_output <- system(eval_cmd, intern = TRUE)

  print(eval_output)

  # Extract log-likelihood value
  if (length(eval_output) == 1) {
    ll_value <- as.numeric(eval_output[1])
    return(ll_value)
  }

  cat("Warning: No valid LL/token value found in evaluation output.\n")
  return(NA_real_)
}

################################################################################
# SECTION 8: TRAIN AND EVALUATE MULTIPLE K VALUES
################################################################################
#
# WARNING: THIS SECTION IS VERY TIME-CONSUMING.
#
# Each k value requires training a full LDA model (800 iterations) on the
# entire corpus and evaluating it on held-out test data. On a large corpus
# (~200k documents) expect each k to take 30-90 minutes depending on hardware.
# With 10 k values (k = 10, 20, ..., 100) the full evaluation run can easily
# take 6-15 hours.
#
# Progress is saved to lda_evaluate_checkpoint.rds after each k, so if the
# run is interrupted it can be resumed without restarting from scratch —
# simply re-run this section and completed k values will be skipped.
#
# The optimal k identified from this evaluation was k = 28. Section 10 trains
# the final model using that value. If you are re-running this pipeline and
# trust the earlier finding, you may skip this section and proceed directly
# to Section 10.
#
################################################################################

cat("\nTraining and evaluating LDA models with different k values...\n")
cat("NOTE: This will take several hours. Progress is checkpointed after each k.\n\n")

# Set Java memory limit
options(java.parameters = "-Xmx8G")

# Evaluate k values from 10 to 100 in steps of 10 to find the optimal number
# of topics. The checkpoint mechanism means this can be safely interrupted
# and resumed — completed k values are skipped on restart.
k_values  <- seq(10, 100, by = 10)
iterations <- 800

# Check if checkpoint exists
checkpoint_file <- "lda_evaluate_checkpoint.rds"
if (file.exists(checkpoint_file)) {
  results <- readRDS(checkpoint_file)
  cat("Resuming from last saved checkpoint...\n")
} else {
  results <- data.frame(k = k_values, LL_token = NA_real_)
}

# Train and evaluate each k value
pb <- txtProgressBar(min = 0, max = length(k_values), style = 3)

for (i in seq_along(k_values)) {
  k <- k_values[i]

  # Skip if already computed
  if (!is.na(results$LL_token[i])) next

  # Train and evaluate model
  results$LL_token[i] <- train_and_eval(k, iterations)

  # Save checkpoint silently
  saveRDS(results, checkpoint_file)

  setTxtProgressBar(pb, i)
}

close(pb)

best_k <- results$k[which.max(results$LL_token)]
cat("\nBest k based on LL/token:", best_k, "\n")

write.csv(results, "lda_evaluate5.csv")
cat("Results saved to lda_evaluate5.csv. Proceed to Section 9.\n")

################################################################################
# SECTION 9: DETERMINE OPTIMAL K USING TWO METHODS
################################################################################

cat("\nDetermining optimal number of topics...\n")

# Load results from checkpoint file if continuing from previous runs
if (file.exists("lda_evaluate5.csv")) {
  lda_evaluate <- read.csv("lda_evaluate5.csv")
}

if (nrow(lda_evaluate) < 3) {
  cat("Skipping k-selection analysis: fewer than 3 k values evaluated.\n")
} else {

################################################################################
# METHOD 1: SEGMENTED REGRESSION (Piecewise Linear Regression)
################################################################################

cat("\nMethod 1: Segmented Regression Analysis\n")

# Function to find optimal k using segmented regression
find_optimal_T_segmented <- function(data) {
  lm_initial <- lm(LL_token ~ k, data = data)
  initial_guess <- median(data$k)

  # Fit segmented regression model
  seg_model <- segmented(lm_initial, seg.Z = ~k, psi = list(k = initial_guess),
                        control = seg.control(n.boot = 100, it.max = 50))

  # Extract breakpoint
  T_star <- round(seg_model$psi[2])

  # Extract confidence intervals
  if (!is.null(confint(seg_model))) {
    T_star_ci <- confint(seg_model)
    if ("k" %in% rownames(T_star_ci)) {
      T_star_ci_low <- round(T_star_ci["k", 1])
      T_star_ci_high <- round(T_star_ci["k", 2])
    } else {
      T_star_ci_low <- NA
      T_star_ci_high <- NA
    }
  } else {
    T_star_ci_low <- NA
    T_star_ci_high <- NA
  }

  return(list(T_star = T_star, T_star_ci_low = T_star_ci_low, T_star_ci_high = T_star_ci_high, model = seg_model))
}

# Run segmented regression
seg_result <- find_optimal_T_segmented(lda_evaluate)
T_star_seg <- seg_result$T_star

# Plot segmented regression results
lda_evaluate$segmented_fit <- predict(seg_result$model)

plot_segmented <- ggplot(lda_evaluate, aes(x = k, y = LL_token)) +
  geom_point(color = "black", alpha = 0.6) +
  geom_line(aes(y = segmented_fit), color = "grey", linewidth = 1) +
  geom_vline(xintercept = T_star_seg, linetype = "dashed", color = "red", linewidth = 1) +
  annotate("text", x = T_star_seg, y = min(lda_evaluate$LL_token) + 0.02,
           label = paste("T* =", T_star_seg), color = "red", hjust = -0.2) +
  ggtitle("Segmented Regression to Determine Optimal Number of Topics") +
  xlab("Number of Topics (k)") +
  ylab("Log-Likelihood per Token (LL/token)") +
  theme_minimal()

print(plot_segmented)

cat("Segmented regression optimal k:", T_star_seg, "\n")

if (!is.na(seg_result$T_star_ci_low) & !is.na(seg_result$T_star_ci_high)) {
  cat("95% Confidence Interval: [", seg_result$T_star_ci_low, ",", seg_result$T_star_ci_high, "]\n")
}

################################################################################
# METHOD 2: RMSE MINIMIZATION (Root Mean Square Error of Two Linear Trends)
################################################################################

cat("\nMethod 2: RMSE Minimization Analysis\n")

# Function to compute RMSE
compute_rmse <- function(actual, predicted) {
  sqrt(mean((actual - predicted)^2))
}

# Function to find optimal k using RMSE minimization
find_optimal_T_rmse <- function(data) {
  possible_T <- data$k[2:(nrow(data) - 1)]
  rmse_values <- numeric(length(possible_T))

  for (i in seq_along(possible_T)) {
    T_star <- possible_T[i]
    data1 <- data[data$k <= T_star, ]
    data2 <- data[data$k > T_star, ]

    lm1 <- lm(LL_token ~ k, data = data1)
    lm2 <- lm(LL_token ~ k, data = data2)

    rmse1 <- compute_rmse(data1$LL_token, predict(lm1, newdata = data1))
    rmse2 <- compute_rmse(data2$LL_token, predict(lm2, newdata = data2))

    rmse_values[i] <- rmse1 + rmse2
  }

  best_index <- which.min(rmse_values)
  best_T <- possible_T[best_index]

  return(best_T)
}

# Run RMSE minimization
T_star_rmse <- find_optimal_T_rmse(lda_evaluate)

# Plot RMSE minimization results
plot_rmse <- ggplot(lda_evaluate, aes(x = k, y = LL_token)) +
  geom_point(color = "blue", alpha = 0.6) +
  geom_line(color = "black", linewidth = 0.7) +
  geom_vline(xintercept = T_star_rmse, linetype = "dashed", color = "red", linewidth = 1) +
  ggtitle("Optimal Number of Topics Using RMSE Minimization") +
  xlab("Number of Topics (k)") +
  ylab("Log-Likelihood per Token (LL/token)") +
  theme_minimal()

print(plot_rmse)

cat("RMSE minimization optimal k:", T_star_rmse, "\n")

} # end k-selection analysis guard

################################################################################
# SECTION 10: TRAIN FINAL MODEL WITH OPTIMAL K AND GENERATE PREDICTIONS
################################################################################

cat("\nTraining final LDA model with k = 28 and generating predictions...\n")

# Define model parameters (k = 28 was selected as optimal)
k <- 28
iterations <- 800

# Define file paths
topic_keys_file <- sprintf("topic-keys-%d.txt", k)
doc_topics_file <- sprintf("doc-topics-%d.txt", k)
evaluator_file <- sprintf("topic-evaluator-%d", k)
inferencer_file <- sprintf("topic-inferencer-%d", k)
test_doc_topics_file <- sprintf("test-doc-topics-%d.txt", k)
merged_file <- sprintf("merged-doc-topics-%d.txt", k)

################################################################################
# TRAIN LDA MODEL ON FULL TRAINING DATA
################################################################################

cat("\nTraining LDA model on full training data...\n")

train_cmd <- sprintf(
  "mallet train-topics --input %s --num-topics %d --num-iterations %d --output-topic-keys %s --output-doc-topics %s --evaluator-filename %s --inferencer-filename %s",
  train_mallet_file, k, iterations, topic_keys_file, doc_topics_file, evaluator_file, inferencer_file
)

system(train_cmd)

################################################################################
# APPLY MODEL TO TEST DATA
################################################################################

cat("\nApplying model to test data...\n")

# Import test data using training data pipe
cmd_test_import <- sprintf(
  "mallet import-file --input %s --output %s --use-pipe-from %s --keep-sequence TRUE --remove-stopwords FALSE",
  test_file, test_mallet_file, train_mallet_file
)

system(cmd_test_import)

# Infer document-topic distributions for test data
infer_cmd <- sprintf(
  "mallet infer-topics --input %s --inferencer %s --output-doc-topics %s",
  test_mallet_file, inferencer_file, test_doc_topics_file
)

system(infer_cmd)

################################################################################
# MERGE TRAINING AND TEST DOCUMENT-TOPIC DISTRIBUTIONS
################################################################################

cat("\nMerging training and test document-topic distributions...\n")

# Read training document-topic matrix
train_doc_topics <- tryCatch({
  read.table(doc_topics_file, header = FALSE, sep = "\t", stringsAsFactors = FALSE)
}, error = function(e) {
  stop("Error reading training doc-topics file: ", doc_topics_file)
})

# Read test document-topic matrix
test_doc_topics <- tryCatch({
  read.table(test_doc_topics_file, header = FALSE, sep = "\t", stringsAsFactors = FALSE)
}, error = function(e) {
  stop("Error reading test doc-topics file: ", test_doc_topics_file)
})

# Verify column consistency
if (ncol(train_doc_topics) != ncol(test_doc_topics)) {
  stop("Mismatch in number of columns between training and test doc-topics files.")
}

# Combine training and test data
merged_doc_topics <- bind_rows(train_doc_topics, test_doc_topics)

# Save merged file
write.table(
  merged_doc_topics, merged_file,
  sep = "\t", row.names = FALSE, col.names = FALSE, quote = FALSE
)

# Final output summary
cat("\nMerged Document-Topic Distributions Saved to:", merged_file, "\n")
cat("Total Documents in Merged File:", nrow(merged_doc_topics), "\n")

cat("\nScript execution complete!\n")
cat("Output files generated:\n")
cat("  - ", merged_file, "\n")
cat("  - ", topic_keys_file, "\n")
cat("  - ", doc_topics_file, "\n")

