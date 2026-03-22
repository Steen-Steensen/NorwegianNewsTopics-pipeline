################################################################################
# PREPROCESSING SCRIPT: Prepare, Filter, and Detect Language in News Articles
################################################################################
#
# DESCRIPTION:
#   This script performs all initial preprocessing steps on the news article
#   corpus:
#     1. Builds combined text fields, removes duplicates, filters short texts,
#        and handles special cases (TV2 duplicate URLs)
#     2. Assigns unique IDs to all articles
#     3. Detects non-Norwegian (primarily English) articles using cld3
#     4. Detects Norwegian Nynorsk articles using a fastText language model
#
#   Text cleaning and lemmatisation are NOT performed here. They are done in
#   script 02 AFTER Nynorsk articles have been translated to Bokmål, ensuring
#   the Bokmål lemmatiser and stopword list are only applied to Bokmål text.
#
# INPUT:
#   - text_corpus_original.csv: Main article dataset with columns:
#       * headline, lead, body_text: components used to build full_text
#       * url, newsroom: identifiers
#       * date_of_publication, article_id: metadata columns
#
# OUTPUT:
#   - alltexts_original_pre_TP.rds: Preprocessed (not yet cleaned) dataset
#   - Remaining_nynorsk.rds: Nynorsk article subset for translation in script 02
#   - remaining_nynorsk.csv: CSV export of Remaining_nynorsk for manual review
#   - filtered_detected_languages.csv: Non-Norwegian articles for manual review
#   - ids_to_remove (vector): Hardcoded list of manually verified non-Norwegian
#       article IDs, used in this script to exclude them from the corpus
#
################################################################################

# Load required libraries
library(dplyr)
library(pbapply)
library(stringr)
library(cld3)
library(parallel)
library(data.table)
library(reticulate)

# Resolve namespace conflict: ensure dplyr::select is used over data.table or other packages
select <- dplyr::select

# ============================================================================
# 0. LOAD DATA
# ============================================================================

# Read main article corpus from CSV
alltexts <- read.csv("text_corpus_original.csv", stringsAsFactors = FALSE, encoding = "UTF-8")
cat("Loaded", nrow(alltexts), "articles from text_corpus_original.csv\n")

# Remove all articles not published in 2023
alltexts <- alltexts[grepl("^2023", alltexts$date_of_publication), ]

# ============================================================================
# 1. BUILD COMBINED FULL TEXT FROM COMPONENTS
# ============================================================================

# Combine headline, lead, and body_text into a single full_text column
alltexts$full_text <- paste(alltexts$headline, alltexts$lead, alltexts$body_text, sep = " ")

# Replace literal "NA" strings (from paste with NA values) with empty strings
alltexts$full_text <- gsub("NA", "", alltexts$full_text)

# Trim excess whitespace
alltexts$full_text <- trimws(alltexts$full_text)

# full_text_NB starts as a copy of full_text; Nynorsk articles will have this
# column replaced with their translated Bokmål text in script 02
alltexts$full_text_NB <- alltexts$full_text

# ============================================================================
# 2. DEDUPLICATION
# ============================================================================

sum(duplicated(alltexts))
dup_rows <- which(duplicated(alltexts))
i <- dup_rows[1]

alltexts[i, ]
alltexts[which(!duplicated(alltexts))[1], ]
# Remove exact duplicate rows
alltexts <- unique(alltexts)

# ============================================================================
# 3. FILTER BY TEXT LENGTH
# ============================================================================

# Function to count words in text
count_words <- function(text) {
  if (is.na(text)) return(0)
  words <- unlist(strsplit(text, "\\s+"))
  return(length(words))
}

# Count words in each article using progress bar
word_counts <- pbsapply(alltexts$full_text, count_words)

# Report and remove texts with 15 words or fewer
short_text_count <- sum(word_counts <= 15, na.rm = TRUE)
cat("Number of texts with 15 words or less:", short_text_count, "\n")

alltexts <- alltexts[!is.na(alltexts$full_text) & word_counts > 15, ]

# ============================================================================
# 4. HANDLE SPECIAL CASE: TV2 DUPLICATE URLS
# ============================================================================

# Some TV2 URLs are identical; make them unique by appending row number
alltexts <- alltexts %>%
  mutate(url = ifelse(url == "https://www.tv2.no/direkte/jpybz/siste-nytt",
                      paste0(url, "_", row_number()),
                      url))

# ============================================================================
# 5. ASSIGN UNIQUE ARTICLE IDS
# ============================================================================

alltexts$ID <- seq_len(nrow(alltexts))
cat("Assigned IDs to", nrow(alltexts), "articles\n")

# ============================================================================
# 6. DETECT NON-NORWEGIAN ARTICLES (cld3)
# ============================================================================

# Extract first 50 words from full_text for efficient language detection
extract_first_50_words <- function(text) {
  if (is.na(text) || text == "") return("")
  words <- unlist(strsplit(text, "\\s+"))
  first_50 <- paste(head(words, 50), collapse = " ")
  return(first_50)
}

cat("Extracting first 50 words from each article for language detection...\n")
alltexts$short_text <- sapply(alltexts$full_text, extract_first_50_words)

# Function to safely detect language using cld3
detect_lang_safe <- function(text) {
  if (is.na(text) || text == "") return(NA)
  tryCatch({
    result <- detect_language(text)
    return(result)
  }, error = function(e) {
    return(NA)
  })
}

# Set up parallel processing with available cores
num_cores <- detectCores() - 1
cat("Using", num_cores, "cores for parallel language detection...\n")

cl <- makeCluster(num_cores)
clusterExport(cl, "detect_lang_safe", envir = environment())
clusterEvalQ(cl, library(cld3))

cat("Detecting language for all articles...\n")
alltexts$detected_language <- parSapply(cl, alltexts$short_text, detect_lang_safe)
stopCluster(cl)

# Frequency table of detected languages
language_frequency <- table(alltexts$detected_language, useNA = "always")
cat("\nDetected language frequency:\n")
print(language_frequency)

# Identify non-Norwegian texts
non_norwegian_mask <- alltexts$detected_language != "no" & !is.na(alltexts$detected_language)
num_non_norwegian <- sum(non_norwegian_mask, na.rm = TRUE)
cat("\nNumber of non-Norwegian articles detected:", num_non_norwegian, "\n")

# Export non-Norwegian articles for manual review
non_norwegian_articles <- alltexts[non_norwegian_mask, ]
review_columns <- c("ID", "detected_language", "headline", "newsroom", "short_text")
non_norwegian_review <- non_norwegian_articles[, review_columns]

write.csv(non_norwegian_review,
          file = "filtered_detected_languages.csv",
          row.names = FALSE,
          na = "")

cat("Non-Norwegian articles exported to filtered_detected_languages.csv\n")
cat("Please review manually to identify confirmed non-Norwegian articles.\n")

# Manually verified IDs to remove (confirmed after reviewing filtered_detected_languages.csv)
ids_to_remove <- c(93338, 106417, 159297)

# Remove non-Norwegian articles from alltexts immediately so they are absent
# from alltexts_original_pre_TP.rds and all downstream datasets
alltexts <- alltexts[!alltexts$ID %in% ids_to_remove, ]
cat("\nRemoved", length(ids_to_remove), "confirmed non-Norwegian articles from alltexts\n")

# Remove temporary working columns
alltexts$short_text <- NULL
alltexts$detected_language <- NULL

# ============================================================================
# 7. DETECT NYNORSK ARTICLES (fastText)
# ============================================================================

# Setup Python environment for fastText
# Set this to the Python executable in your virtual environment that has
# fasttext and huggingface_hub installed.
# To find your Python path, run in a terminal: which python  (macOS/Linux)
#                                           or: where python  (Windows)
# Examples:
#   macOS/Linux: Sys.setenv(RETICULATE_PYTHON = "~/.virtualenvs/fasttext_env/bin/python")
#   Windows:     Sys.setenv(RETICULATE_PYTHON = "C:/Users/YourName/envs/fasttext_env/Scripts/python.exe")
Sys.setenv(RETICULATE_PYTHON = "PATH/TO/YOUR/PYTHON")

# Optional sanity check
py_config()

# Import Python packages
hf_hub_download <- import("huggingface_hub")$hf_hub_download
fasttext <- import("fasttext")

# Download and load the Norwegian Bokmål/Nynorsk classifier model
model_path <- hf_hub_download("NbAiLab/nb-nbnn-lid", "nb-nbnn-lid.ftz")
model <- fasttext$load_model(model_path)

# Function to detect if text is Norwegian Nynorsk (nno)
detect_norwegian_variant <- function(text) {
  if (is.na(text) || !nzchar(trimws(text))) return(NA)

  text <- gsub("\n", " ", text)
  text <- gsub("\r", " ", text)
  text <- trimws(text)

  prediction <- model$predict(text)
  lang <- sub("__label__", "", prediction[[1]][1])
  return(lang == "nno")
}

# Apply language detection to all texts with progress bar
cat("Detecting Norwegian language variants...\n")
alltexts$Is_Nynorsk_in_NB <- pbsapply(alltexts$full_text_NB, detect_norwegian_variant)

# Summary by newsroom
newsroom_summary <- alltexts %>%
  group_by(newsroom) %>%
  summarise(
    total_texts = n(),
    nynorsk_texts = sum(Is_Nynorsk_in_NB, na.rm = TRUE)
  )

cat("\nNynorsk texts by newsroom:\n")
print(newsroom_summary)

# Extract Nynorsk articles for translation in script 02
# Note: full_text_NB is retained so script 02 can translate from raw text
Remaining_nynorsk <- alltexts[alltexts$Is_Nynorsk_in_NB == TRUE, ] %>%
  select(-Is_Nynorsk_in_NB)

cat("\nNynorsk detection complete.\n")
cat("Total Nynorsk texts identified:", nrow(Remaining_nynorsk), "\n")

# Remove the detection flag from the main dataset
alltexts$Is_Nynorsk_in_NB <- NULL

# ============================================================================
# 8. SAVE OUTPUT
# ============================================================================

# Save main dataset (no cleaned_text yet — cleaning happens in script 02
# after Nynorsk articles have been translated to Bokmål)
saveRDS(alltexts, "alltexts_original_pre_TP.rds")
cat("\nPreprocessing complete. Output saved to alltexts_original_pre_TP.rds\n")

# Save Nynorsk subset for translation
saveRDS(Remaining_nynorsk, "Remaining_nynorsk.rds")
write.csv(Remaining_nynorsk, "remaining_nynorsk.csv", row.names = FALSE)
cat("Nynorsk subset saved to Remaining_nynorsk.rds and remaining_nynorsk.csv\n")
cat("Next: run 02_translate_nynorsk_clean.R\n")
