################################################################################
# CREATE DOCUMENT-TERM MATRIX (DTM) FOR LDA ANALYSIS
################################################################################
#
# PURPOSE:
#   Preprocesses and tokenizes text data, removes stopwords, and creates a
#   Document-Feature Matrix (DFM) and Document-Term Matrix (DTM) optimized
#   for Latent Dirichlet Allocation (LDA) topic modeling.
#
# INPUT:
#   - alltexts_preprocessed.rds: Data frame from 02_translate_nynorsk_clean.R with columns:
#     * ID: Document identifier
#     * cleaned_text: Preprocessed text (Nynorsk articles translated to Bokmål)
#
# OUTPUT:
#   - dtm_lda_optimized.rds: Document-Term Matrix in tm package format
#   - tokens_list.rds: List of tokenized documents (one element per document ID)
#   - dfm_all.rds: Quanteda DFM object (intermediate, for reference)
#
# NOTES:
#   - Removes Norwegian stopwords
#   - Filters low-frequency terms (min_termfreq = 5)
#   - Uses quanteda and tm packages for efficient matrix operations
#
################################################################################

# Load required libraries
library(dplyr)
library(tidytext)
library(quanteda)
library(pbapply)
library(Matrix)
library(tm)
library(textstem)
library(stringr)
library(data.table)

# Resolve namespace conflict: ensure dplyr::select is used over data.table or other packages
select <- dplyr::select

# Load Norwegian stopwords
norwegian_stopwords <- stopwords("no")

cat("\nLoading preprocessed texts...\n")
# Read from the output of 02_translate_nynorsk_clean.R (all Nynorsk translated to Bokmål)
alltexts <- readRDS("alltexts_preprocessed.rds")

# Ensure alltexts is a data frame
if (!is.data.frame(alltexts)) {
  alltexts <- as.data.frame(alltexts, stringsAsFactors = FALSE)
}

# Validate required columns
if (!"cleaned_text" %in% colnames(alltexts)) {
  stop("ERROR: 'cleaned_text' column not found in alltexts.")
}

if (!"ID" %in% colnames(alltexts)) {
  stop("ERROR: 'ID' column not found in alltexts.")
}

cat("Successfully loaded alltexts with", nrow(alltexts), "documents.\n")

saveRDS(alltexts, "alltexts.rds")
gc()

################################################################################
# SECTION 1: TOKENIZE TEXTS AND REMOVE STOPWORDS
################################################################################

cat("\nTokenizing texts...\n")

# Tokenize documents and remove Norwegian stopwords
tokens_df <- alltexts %>%
  select(ID, cleaned_text) %>%
  unnest_tokens(word, cleaned_text) %>%
  filter(!word %in% norwegian_stopwords)

# Validate tokenization results
if (!"word" %in% colnames(tokens_df) || !"ID" %in% colnames(tokens_df)) {
  stop("ERROR: Tokenization failed. 'word' or 'ID' column is missing.")
}

cat("Tokenization complete! Total tokens:", nrow(tokens_df), "\n")

saveRDS(tokens_df, "backup_alltokens.rds")
gc()

################################################################################
# SECTION 2: SPLIT TOKENS BY DOCUMENT
################################################################################

cat("\nSplitting words by document...\n")

tokens_dt <- as.data.table(tokens_df)
unique_docs <- unique(tokens_dt$ID)

# Create list of token vectors, one per document
tokens_list <- pblapply(unique_docs, function(doc) {
  tokens_dt[ID == doc, word]
})

names(tokens_list) <- unique_docs

saveRDS(tokens_list, "tokens_list.rds")

cat("Document-wise token split complete! Total documents:", length(tokens_list), "\n")

gc()

################################################################################
# SECTION 3: CONVERT TO QUANTEDA TOKENS
################################################################################

cat("\nConverting tokens to quanteda format...\n")

# Create quanteda tokens object with progress tracking
batch_size <- ceiling(length(tokens_list) / 100)

pb <- txtProgressBar(min = 0, max = length(tokens_list), style = 3)

tokens_all <- list()
for (i in seq(1, length(tokens_list), by = batch_size)) {
  batch_end <- min(i + batch_size - 1, length(tokens_list))
  tokens_all[i:batch_end] <- quanteda::tokens(tokens_list[i:batch_end])
  setTxtProgressBar(pb, batch_end)
}
close(pb)

tokens_all <- quanteda::tokens(tokens_all)

cat("Tokenization complete! Total documents:", length(tokens_all), "\n")

gc()

saveRDS(tokens_all, "backup_alltokens.rds")
saveRDS(tokens_df, "backup_tokens_df.rds")

################################################################################
# SECTION 4: CREATE DOCUMENT-FEATURE MATRIX (DFM)
################################################################################

cat("\nCreating Document-Feature Matrix (DFM)...\n")

pb <- txtProgressBar(min = 0, max = length(tokens_all), style = 3)
dfm_all <- dfm(tokens_all)
setTxtProgressBar(pb, length(tokens_all))
close(pb)

rm(tokens_all)
gc()

cat("DFM created! Dimensions:", dim(dfm_all), "\n")

################################################################################
# SECTION 5: ASSIGN DOCUMENT IDS AND GROUP BY DOCUMENT
################################################################################

cat("\nAssigning Document IDs to DFM...\n")

# Add document factor variable
docvars(dfm_all, "document") <- as.factor(seq_len(ndoc(dfm_all)))

if (!"document" %in% colnames(docvars(dfm_all))) {
  stop("ERROR: Document IDs are missing from dfm_all.")
}

cat("Document IDs assigned successfully!\n")

cat("\nGrouping DFM by document ID...\n")
dfm_all <- dfm_group(dfm_all, groups = docvars(dfm_all, "document"))

cat("Grouping complete! Total documents:", ndoc(dfm_all), "\n")

################################################################################
# SECTION 6: TRIM LOW-FREQUENCY TERMS
################################################################################

cat("\nRemoving low-frequency words (min_termfreq = 5)...\n")

pb <- txtProgressBar(min = 0, max = nfeat(dfm_all), style = 3)
dfm_all <- dfm_trim(dfm_all, min_termfreq = 5)
setTxtProgressBar(pb, nfeat(dfm_all))
close(pb)

cat("Low-frequency words removed! Remaining terms:", nfeat(dfm_all), "\n")

saveRDS(dfm_all, "dfm_all.rds")
gc()

################################################################################
# SECTION 7: CONVERT TO TM DOCUMENT-TERM MATRIX FORMAT
################################################################################

cat("\nConverting DFM to Document-Term Matrix (DTM)...\n")

dtm <- convert(dfm_all, to = "tm")

rm(dfm_all)
gc()

cat("DTM successfully created! Dimensions:", dim(dtm), "\n")

################################################################################
# SECTION 8: SAVE DTM
################################################################################

cat("\nSaving DTM to file...\n")

saveRDS(dtm, "dtm_lda_optimized.rds")

cat("DTM saved to dtm_lda_optimized.rds!\n")
cat("Script execution complete!\n")
