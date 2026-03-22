# ============================================================================
# Script: 11_match_facebook_cosine.R
# Purpose: Match Facebook posts with most similar news stories using
#          precomputed text similarity (cosine similarity via TF-IDF)
#
# Prerequisites:
#   - 07_cosine_similarity_function.R must be sourced (defines match_some_posts function)
#   - newsroom_results/ folder must exist with precomputed newsroom RDS files
#   - These can be generated using precompute_newsroom_tfidf() from 08_match_instagram.R
#
# Input:
#   - FB_Data_filtered_remaining: dataframe with Facebook posts produced by
#     10_match_facebook_url_id.R. The newsroom column must already match
#     the newsroom keys used in alltexts (see Facebook_Data.csv requirements).
#     Required columns: newsroom, message, post_created, url, link, final_link,
#                       total_interactions, overperforming
#   - alltexts_TP: dataframe with news articles (url, headline, lead)
#   - newsroom_results/ folder: RDS files with precomputed DTM per newsroom
#
# Output:
#   - final_results_FB.rds: complete matching results
#   - final_results_FB.xlsx: full results export
#   - sampled_results_FB.xlsx: 2% sample per newsroom (min 2 posts) for review
#
# Dependencies: dplyr, stringr, tm, stopwords, pbapply, writexl, text2vec
# ============================================================================

library(stringr)
library(dplyr)
library(tm)
library(stopwords)
library(pbapply)
library(writexl)

# Resolve namespace conflict: ensure dplyr::select is used over data.table or other packages
select <- dplyr::select

# Source the cosine similarity matching function defined in script 07
source("07_cosine_similarity_function.R")

# Load article metadata (needed for post-processing joins)
if (!exists("alltexts_TP")) {
  alltexts_TP <- readRDS("alltexts_with_TP_distribution.rds")
}

# Load the unmatched Facebook posts produced by script 10
if (!exists("FB_Data_filtered_remaining")) {
  FB_Data_filtered_remaining <- readRDS("FB_Data_filtered_remaining.rds")
}

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

# Apply cleaning to Facebook data
process_FBData <- function(dataframe) {
  dataframe$processed_message <- pbsapply(dataframe$message, clean_and_tokenize)
  return(dataframe)
}

# Process the Facebook dataset
FB_Data_filtered_remaining <- process_FBData(FB_Data_filtered_remaining)

# Reorder columns to place processed_message next to raw message
FB_Data_filtered_remaining <- FB_Data_filtered_remaining[,
  c("message", "processed_message",
    setdiff(names(FB_Data_filtered_remaining), c("message", "processed_message")))
]

# ============================================================================
# SIMILARITY MATCHING LOOP
# ============================================================================

# Extract unique newsrooms from Facebook data
newsrooms <- unique(FB_Data_filtered_remaining$newsroom)

# Initialize progress tracking (separate file from Instagram/TikTok runs)
progress_file <- "processed_newsrooms_fb.txt"
if (file.exists(progress_file)) {
  processed_newsrooms <- readLines(progress_file)
} else {
  processed_newsrooms <- character()
}

# Initialize results dataframe or load existing partial results
results_file <- "final_results_FB.rds"
if (file.exists(results_file)) {
  final_results_FB <- readRDS(results_file)
} else {
  final_results_FB <- data.frame(
    newsroom = character(),
    processed_message = character(),
    post_created = as.Date(character()),
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

  # Filter Facebook data for current newsroom
  new_FBData <- FB_Data_filtered_remaining %>%
    filter(newsroom == selected_newsroom & !is.na(processed_message) & processed_message != "")

  if (nrow(new_FBData) == 0) {
    print(paste("No valid data for newsroom:", selected_newsroom))
    next
  }

  # Load precomputed newsroom data (DTM, vocabulary, metadata)
  newsroom_data <- readRDS(file.path("newsroom_results", paste0(selected_newsroom, ".rds")))

  # Match Facebook posts to news articles using cosine similarity
  results <- match_some_posts(
    some_data = new_FBData,
    newsroom_data = newsroom_data,
    text_column = "processed_message",
    date_column = "post_created",
    similarity_threshold = 0.05,
    batch_size = 2000
  )

  # Prepare results: ensure data types and select columns
  results <- results %>%
    mutate(
      newsroom = selected_newsroom,
      post_created = as.Date(post_created),
      similarity_score = as.numeric(similarity_score)
    ) %>%
    select(newsroom, processed_message, post_created, similar_url, similarity_score,
           url, link, final_link, total_interactions, overperforming)

  # Append to results
  final_results_FB <- bind_rows(final_results_FB, results)

  # Save progress
  saveRDS(final_results_FB, results_file)

  processed_newsrooms <- c(processed_newsrooms, selected_newsroom)
  writeLines(processed_newsrooms, progress_file)

  # Clean up memory
  rm(new_FBData, newsroom_data, results)
  gc()

  print(paste("Finished processing newsroom:", selected_newsroom))
}

print("All newsrooms have been processed.")

# ============================================================================
# POST-PROCESSING AND OUTPUT
# ============================================================================

# Remove duplicate rows
final_results_FB <- unique(final_results_FB)

# Join with news article metadata (headline, lead)
final_results_FB <- final_results_FB %>%
  left_join(alltexts_TP %>% select(url, headline, lead),
            by = c("similar_url" = "url"))

# Join with original Facebook message text
final_results_FB <- final_results_FB %>%
  left_join(FB_Data_filtered_remaining %>% select(url, message), by = "url")

# Reorder columns for readability
final_results_FB <- final_results_FB %>%
  select(newsroom, message, headline, lead, similar_url, similarity_score, everything())

# Export full results to RDS and Excel
saveRDS(final_results_FB, results_file)
write_xlsx(final_results_FB, "final_results_FB.xlsx")

# Create sampled dataset: 2% per newsroom (minimum 2 posts) for manual review
sampled_results_FB <- final_results_FB %>%
  group_by(newsroom) %>%
  group_map(~ {
    num_rows <- nrow(.x)
    sample_size <- max(2, ceiling(num_rows * 0.02))
    .x %>% sample_n(sample_size)
  }) %>%
  bind_rows()

sampled_results_FB <- ungroup(sampled_results_FB)

print("Sampled dataframe created with 2% of rows for each newsroom (minimum 2 posts).")

# Export sampled results
write_xlsx(sampled_results_FB, "sampled_results_FB.xlsx")
