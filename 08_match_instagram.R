# ============================================================================
# Script: 08_match_instagram.R
# Purpose: Match Instagram posts with most similar news stories using
#          precomputed text similarity (cosine similarity via TF-IDF)
#
# Prerequisites:
#   - 07_cosine_similarity_function.R must be sourced (defines match_some_posts function)
#   - Instagram_Data.csv must have the Account column already matching the
#     newsroom naming convention used in alltexts (lowercase, no spaces or
#     special characters, e.g. "tv2nyheter"). Recode this column before running.
#
# Input:
#   - Instagram_Data.csv: Instagram posts with Account column matching
#                         alltexts newsroom keys. Other columns: Description,
#                         PostCreated, URL, Link, TotalInteractions, OverperformingScore
#   - alltexts_with_TP_distribution.rds: news articles (newsroom, url, headline, lead, body_text)
#     produced by 06_add_topic_values.R
#
# Output:
#   - final_results_Insta.rds: complete matching results
#   - final_results_Insta.xlsx: full results export
#   - sampled_results_Insta.xlsx: 5% sample per newsroom (min 2 posts) for review
#   - newsroom_results/: folder with precomputed DTM and metadata per newsroom
#
# Dependencies: dplyr, stringr, tm, stopwords, pbapply, text2vec, writexl
# ============================================================================

library(stringr)
library(dplyr)
library(tm)
library(stopwords)
library(pbapply)
library(text2vec)
library(Matrix)
library(writexl)

# Resolve namespace conflict: ensure dplyr::select is used over data.table or other packages
select <- dplyr::select

# Source the cosine similarity matching function defined in script 07
source("07_cosine_similarity_function.R")

# ============================================================================
# LOAD DATA
# ============================================================================

# Read Instagram posts from CSV; rename Account to Newsroom to match pipeline conventions
InsData <- read.csv("Instagram_Data.csv", stringsAsFactors = FALSE, encoding = "UTF-8")
InsData <- InsData %>% rename(Newsroom = Account)
cat("Loaded", nrow(InsData), "Instagram posts from Instagram_Data.csv\n")

# Load article dataset (produced by 06_add_topic_values.R)
alltexts_TP <- readRDS("alltexts_with_TP_distribution.rds")

# Derive the two article dataframe forms that the matching functions expect:
# all_text: full article data including body_text and date (for building TF-IDF matrices)
all_text <- alltexts_TP %>% select(newsroom, url, headline, lead, body_text, date_of_publication)
# all_text_agg: url + headline + lead only (for joining metadata onto results)
all_text_agg <- alltexts_TP %>% select(url, headline, lead)

# ============================================================================
# TEXT PREPROCESSING FUNCTIONS
# ============================================================================

# Clean and tokenize function: lowercase, remove punctuation/digits/emojis/stopwords
clean_and_tokenize <- function(text) {
  text <- tolower(text)
  text <- str_replace_all(text, "[[:punct:]]", " ")
  text <- str_replace_all(text, "[[:digit:]]", " ")
  text <- str_replace_all(text, "[\\p{Emoji}\\p{Extended_Pictographic}]", "")
  text <- removeWords(text, stopwords("no"))
  text <- str_replace_all(text, "\\s+", " ")
  text <- trimws(text)
  tokens <- unlist(str_split(text, "\\s+"))
  return(paste(tokens, collapse = " "))
}

# Apply cleaning to news text (combined lead, headline, body)
process_all_text <- function(dataframe) {
  dataframe$processed_text <- pbsapply(dataframe$full_text, clean_and_tokenize)
  return(dataframe)
}

# Apply cleaning to Instagram posts
process_InsData <- function(dataframe) {
  dataframe$processed_description <- pbsapply(dataframe$Description, clean_and_tokenize)
  return(dataframe)
}

# ============================================================================
# DATA PREPARATION
# ============================================================================

# Process Instagram data
InsData <- process_InsData(InsData)

# Create full text from news components
all_text <- all_text %>%
  mutate(full_text = paste(lead, headline, body_text, sep = " "))

# Process news text
all_text <- process_all_text(all_text)

# Filter out short articles (minimum 50 words)
all_text <- all_text %>%
  filter(str_count(processed_text, "\\w+") >= 50)

# ============================================================================
# PRECOMPUTE NEWSROOM TF-IDF MATRICES
# ============================================================================
# This function must be run before the matching loop below.
# It generates RDS files in newsroom_results/ that store the document-term
# matrix and vocabulary for each newsroom. These are used for efficient
# cosine similarity matching in match_some_posts().

precompute_newsroom_tfidf <- function(data, results_dir = "newsroom_results", batch_size = 5000) {
  library(text2vec)
  library(Matrix)
  library(dplyr)

  # Create results directory if it doesn't exist
  if (!dir.exists(results_dir)) {
    dir.create(results_dir)
  }

  print("Starting precompute process...")

  # Group data by newsroom
  newsroom_groups <- data %>% group_split(newsroom)
  print(paste("Total newsrooms to process:", length(newsroom_groups)))

  for (group in newsroom_groups) {
    newsroom_name <- unique(group$newsroom)
    print(paste("Processing newsroom:", newsroom_name))

    # Create text iterator for efficient processing
    text_iterator <- text2vec::itoken(
      group$processed_text,
      preprocessor = tolower,
      tokenizer = text2vec::word_tokenizer,
      ids = group$url,
      progressbar = FALSE
    )

    # Create vocabulary with filtering
    # term_count_min=1 includes rare terms; doc_proportion_max=0.5 excludes overly common terms
    vocabulary <- text2vec::create_vocabulary(
      text_iterator,
      term_count_min = 1,
      doc_proportion_max = 0.5
    )

    if (nrow(vocabulary) == 0) {
      print(paste("Empty vocabulary for newsroom:", newsroom_name))
      next
    } else {
      print(paste("Vocabulary size for", newsroom_name, ":", nrow(vocabulary)))
    }

    # Save vocabulary for reference
    vocab_file <- file.path(results_dir, paste0(newsroom_name, "_vocabulary.rds"))
    saveRDS(vocabulary, vocab_file)
    print(paste("Saved vocabulary for newsroom:", newsroom_name))

    # Create vectorizer from vocabulary
    vectorizer <- text2vec::vocab_vectorizer(vocabulary)

    # Create document-term matrix
    dtm <- text2vec::create_dtm(text_iterator, vectorizer)

    if (is.null(dtm) || nrow(dtm) == 0 || ncol(dtm) == 0) {
      print(paste("Empty or invalid DTM for newsroom:", newsroom_name))
      next
    } else {
      print(paste("DTM created for newsroom:", newsroom_name,
                  "with", nrow(dtm), "documents and", ncol(dtm), "terms."))
    }

    # Save DTM and metadata for matching
    # Use date_of_publication (the actual column name in the article corpus)
    saveRDS(list(
      dtm = dtm,
      urls = group$url,
      dates = as.Date(group$date_of_publication),
      vectorizer = vectorizer
    ), file.path(results_dir, paste0(newsroom_name, ".rds")))

    print(paste("Saved DTM and metadata for newsroom:", newsroom_name))
  }

  print("Precompute process completed.")
}

# ============================================================================
# PRECOMPUTE NEWSROOM TF-IDF MATRICES (run once; skips existing files)
# ============================================================================

precompute_newsroom_tfidf(all_text)

# ============================================================================
# SIMILARITY MATCHING LOOP
# ============================================================================

# Extract unique newsrooms from Instagram data
newsrooms <- unique(InsData$Newsroom)

# Initialize progress tracking (separate file per platform to avoid collisions)
progress_file <- "processed_newsrooms_insta.txt"
if (file.exists(progress_file)) {
  processed_newsrooms <- readLines(progress_file)
} else {
  processed_newsrooms <- character()
}

# Initialize results dataframe or load existing partial results
results_file <- "final_results_Insta.rds"
if (file.exists(results_file)) {
  final_results_Insta <- readRDS(results_file)
} else {
  final_results_Insta <- data.frame(
    Newsroom = character(),
    processed_description = character(),
    PostCreated = as.Date(character()),
    similar_url = character(),
    similarity_score = numeric(),
    stringsAsFactors = FALSE
  )
}

# Process each newsroom
for (selected_newsroom in newsrooms) {
  # Skip already processed newsrooms
  if (selected_newsroom %in% processed_newsrooms) {
    print(paste("Skipping already processed newsroom:", selected_newsroom))
    next
  }

  print(paste("Processing newsroom:", selected_newsroom))

  # Filter Instagram data for current newsroom
  new_InsData <- InsData %>%
    filter(Newsroom == selected_newsroom & !is.na(processed_description) & processed_description != "")

  if (nrow(new_InsData) == 0) {
    print(paste("No valid data for newsroom:", selected_newsroom))
    next
  }

  # Load precomputed newsroom data (DTM, vocabulary, metadata)
  newsroom_data <- readRDS(file.path("newsroom_results", paste0(selected_newsroom, ".rds")))

  # Match Instagram posts to news articles using cosine similarity
  results <- match_some_posts(
    some_data = new_InsData,
    newsroom_data = newsroom_data,
    text_column = "processed_description",
    date_column = "PostCreated",
    similarity_threshold = 0.05,
    batch_size = 2000
  )

  # Prepare results: ensure data types and select columns
  results <- results %>%
    mutate(
      Newsroom = selected_newsroom,
      PostCreated = as.Date(PostCreated),
      similarity_score = as.numeric(similarity_score)
    ) %>%
    select(Newsroom, processed_description, PostCreated, similar_url, similarity_score,
           URL, Link, TotalInteractions, OverperformingScore)

  # Append to results
  final_results_Insta <- bind_rows(final_results_Insta, results)

  # Save progress
  saveRDS(final_results_Insta, results_file)

  processed_newsrooms <- c(processed_newsrooms, selected_newsroom)
  writeLines(processed_newsrooms, progress_file)

  # Clean up memory
  rm(new_InsData, newsroom_data, results)
  gc()

  print(paste("Finished processing newsroom:", selected_newsroom))
}

print("All newsrooms have been processed.")

# ============================================================================
# POST-PROCESSING AND OUTPUT
# ============================================================================

# Join with news article metadata (headline, lead)
final_results_Insta <- final_results_Insta %>%
  left_join(all_text_agg %>% select(url, headline, lead),
            by = c("similar_url" = "url"))

# Join with original Instagram description
final_results_Insta <- final_results_Insta %>%
  left_join(InsData %>% select(URL, Description), by = "URL")

# Reorder columns for readability
final_results_Insta <- final_results_Insta %>%
  select(Description, headline, lead, similar_url, similarity_score, everything())

# Export full results to RDS and Excel
saveRDS(final_results_Insta, results_file)
write_xlsx(final_results_Insta, "final_results_Insta.xlsx")

# Create sampled dataset: 5% per newsroom (minimum 2 posts) for manual review
sampled_results_Insta <- final_results_Insta %>%
  group_by(Newsroom) %>%
  group_map(~ {
    num_rows <- nrow(.x)
    sample_size <- max(2, ceiling(num_rows * 0.05))
    .x %>% sample_n(sample_size)
  }) %>%
  bind_rows()

sampled_results_Insta <- ungroup(sampled_results_Insta)

print("Sampled dataframe created with 5% of rows for each newsroom (minimum 2 posts).")

# Export sampled results
write_xlsx(sampled_results_Insta, "sampled_results_Insta.xlsx")
