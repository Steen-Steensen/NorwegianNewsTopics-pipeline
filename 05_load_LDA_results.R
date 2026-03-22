################################################################################
# LDA RESULTS LOADING SCRIPT: Integrate Topic Distribution with Article Texts
################################################################################
#
# DESCRIPTION:
#   This script loads LDA (Latent Dirichlet Allocation) topic modeling results
#   from MALLET output files and integrates them with the preprocessed article
#   dataset. It prepares the combined dataset for downstream topic analysis.
#
#   Topic modeling was performed in script 04_LDA_topic_modeling.R using MALLET,
#   generating document-topic distributions and topic keywords that are loaded
#   and processed here.
#
#
# INPUT:
#   - alltexts_preprocessed.rds: Final preprocessed article dataset from script 02
#       with columns: full_text, full_text_NB, cleaned_text, url, newsroom,
#       headline, lead, body_text, ID, and other metadata
#       (cleaned_text contains Bokmål translations for all Nynorsk articles)
#   - merged-doc-topics-28.txt: MALLET output file containing document-topic
#       distributions (tab-delimited, no header). Columns: ID_TP, ID, Topic1-Topic28
#   - topic-keys-28.txt: MALLET output file containing topic keywords
#       (tab-delimited, no header). Columns: Topic, keywords (and intermediate column removed)
#
# OUTPUT:
#   - alltexts.rds: Article dataset with columns organised with ID first and
#       temporary processing columns removed. Ready for joining with topic
#       distribution in subsequent analysis.
#   - topic_distribution.rds: Document-topic distribution matrix containing
#       probabilities for each article across 28 topics.
#
# DEPENDENCIES:
#   - dplyr: Data manipulation and filtering
#   - data.table: Efficient data loading and handling
#
################################################################################

# Load required libraries
library(dplyr)
library(data.table)

# Resolve namespace conflict: ensure dplyr::select is used over data.table or other packages
select <- dplyr::select

# ============================================================================
# 0. LOAD PREPROCESSED ARTICLES
# ============================================================================

# Load the final preprocessed dataset from script 02 (Nynorsk translated to Bokmål)
alltexts <- readRDS("alltexts_preprocessed.rds")
cat("Loaded", nrow(alltexts), "articles from alltexts_preprocessed.rds\n")

# Rename 'document' column to 'ID' if present (handle naming variations)
if ("document" %in% names(alltexts)) {
  alltexts <- alltexts %>%
    rename(ID = document)
}

# Verify ID column exists
if (!"ID" %in% names(alltexts)) {
  stop("Error: ID column not found in alltexts dataset")
}

# ============================================================================
# 1. LOAD LDA TOPIC DISTRIBUTION FROM MALLET OUTPUT
# ============================================================================

# Load document-topic distribution from MALLET
# Format: tab-delimited, no header, columns are: ID_TP, ID, Topic1-Topic28
Topic_distribution <- fread("merged-doc-topics-28.txt", sep = "\t", header = FALSE)

cat("Loaded", nrow(Topic_distribution), "documents with topic distributions\n")

# Rename columns: V1->ID_TP, V2->ID, V3-V30->Topic1-Topic28
column_names <- c("ID_TP", "ID", paste0("Topic", 1:28))
setnames(Topic_distribution, names(Topic_distribution), column_names)

# Strip the "doc" prefix from ID and convert to numeric
# MALLET output format: docN where N is the numeric ID
Topic_distribution$ID <- as.numeric(gsub("doc", "", Topic_distribution$ID))

cat("Topic distribution columns renamed and ID processed\n")

# ============================================================================
# 2. LOAD TOPIC KEYWORDS FROM MALLET OUTPUT
# ============================================================================

# Load topic keywords from MALLET output
# Format: tab-delimited, no header, columns are: Topic, intermediate column, keywords
topic_keys_raw <- fread("topic-keys-28.txt", sep = "\t", header = FALSE)

cat("Loaded", nrow(topic_keys_raw), "topics with keywords\n")

# Remove the second column (intermediate data from MALLET) and rename
topic_keys <- topic_keys_raw[, c(1, 3), with = FALSE]
setnames(topic_keys, c("V1", "V3"), c("Topic", "keywords"))

# Convert Topic to numeric for consistency
topic_keys$Topic <- as.numeric(topic_keys$Topic)

cat("Topic keywords processed and cleaned\n")

# ============================================================================
# 3. CLEAN AND PREPARE ALLTEXTS DATASET
# ============================================================================

# Remove columns that will be added cleanly in downstream scripts
# These columns may have data integrity issues or will be regenerated
columns_to_remove <- c(
  names(alltexts)[grepl("^Topic", names(alltexts))],  # Remove any Topic* columns
  "full_text",           # Original combined text (not used in downstream analysis)
  "full_text_NB",        # Norwegian variant (not needed after preprocessing)
  "Is_Nynorsk_in_NB"     # Language detection marker (not used in final analysis)
)

# Only remove columns that actually exist
columns_to_remove <- intersect(columns_to_remove, names(alltexts))

if (length(columns_to_remove) > 0) {
  alltexts <- alltexts %>%
    dplyr::select(-all_of(columns_to_remove))
}

# Move ID column to first position for clarity
alltexts <- alltexts %>%
  dplyr::select(ID, everything())

cat("alltexts cleaned: ID column moved to first position\n")
cat("alltexts now has", ncol(alltexts), "columns\n")

# ============================================================================
# 5. SAVE OUTPUT DATASETS
# ============================================================================

# Save cleaned alltexts dataset
saveRDS(alltexts, "alltexts.rds")
cat("Cleaned alltexts saved to alltexts.rds\n")

# Save topic distribution
saveRDS(Topic_distribution, "topic_distribution.rds")
cat("Topic distribution saved to topic_distribution.rds\n")

cat("\nLDA results loading complete.\n")
cat("Ready for downstream topic analysis in script 06.\n")
