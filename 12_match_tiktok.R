# ============================================================================
# Script: 12_match_tiktok.R
# Purpose: Match TikTok posts with most similar news stories using
#          precomputed text similarity (cosine similarity via TF-IDF)
#
# Prerequisites:
#   - 07_cosine_similarity_function.R must be sourced (defines match_some_posts function)
#   - newsroom_results/ folder must exist with precomputed newsroom RDS files
#   - These can be generated using precompute_newsroom_tfidf() from 08_match_instagram.R
#
# Input:
#   - Adresseavisen_tiktok.csv, VG_tiktok.csv, romerikesblad_tiktok.csv,
#     Frederikstad_blad_tiktok.csv: TikTok post data with columns:
#     username, video_description, create_time, like_count, view_count,
#     comment_count, share_count, id
#   - alltexts_TP: dataframe with news articles (url, headline, lead)
#   - newsroom_results/ folder: RDS files with precomputed DTM per newsroom
#
# Output:
#   - final_results_Tiktok.rds: complete matching results
#   - final_results_Tiktok.xlsx: full results export
#   - sampled_results_Tiktok.xlsx: 10% sample per newsroom (min 20 posts) for review
#
# Dependencies: dplyr, stringr, tm, stopwords, pbapply, writexl, text2vec
# ============================================================================

library(dplyr)
library(stringr)
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

# ============================================================================
# LOAD DATA
# ============================================================================

# Read TikTok data for each of the four newsrooms from CSV files
Adresseavisen_tiktok  <- read.csv("Adresseavisen_tiktok.csv",  stringsAsFactors = FALSE, encoding = "UTF-8")
VG_tiktok             <- read.csv("VG_tiktok.csv",             stringsAsFactors = FALSE, encoding = "UTF-8")
romerikesblad_tiktok  <- read.csv("romerikesblad_tiktok.csv",  stringsAsFactors = FALSE, encoding = "UTF-8")
Frederikstad_blad_tiktok <- read.csv("Frederikstad_blad_tiktok.csv", stringsAsFactors = FALSE, encoding = "UTF-8")
cat("Loaded TikTok data:",
    nrow(Adresseavisen_tiktok), "Adresseavisen,",
    nrow(VG_tiktok), "VG,",
    nrow(romerikesblad_tiktok), "Romerikesblad,",
    nrow(Frederikstad_blad_tiktok), "Fredrikstad Blad rows\n")

# ============================================================================
# DATA COMBINATION
# ============================================================================

# Combine TikTok data from multiple newsroom accounts into a single dataframe
Tiktok_data <- bind_rows(Adresseavisen_tiktok, VG_tiktok, romerikesblad_tiktok, Frederikstad_blad_tiktok)

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

# Apply cleaning to TikTok posts
process_TiktokData <- function(dataframe) {
  dataframe$processed_description <- pbsapply(dataframe$video_description, clean_and_tokenize)
  return(dataframe)
}

# Process the TikTok dataset
Tiktok_data <- process_TiktokData(Tiktok_data)

# Reorder columns to place processed_description next to raw video_description
Tiktok_data <- Tiktok_data[,
  c("video_description", "processed_description",
    setdiff(names(Tiktok_data), c("video_description", "processed_description")))
]

# ============================================================================
# NEWSROOM MAPPING
# ============================================================================

# Map TikTok username to standardized newsroom names
Tiktok_data <- Tiktok_data %>%
  mutate(newsroom = case_when(
    username == "vgnett" ~ "vg",
    username == "adresseavisen" ~ "adresseavisen",
    username == "frblad" ~ "fredriksstadblad",
    username == "romerikesblad" ~ "romerikesblad",
    TRUE ~ NA_character_
  ))

# ============================================================================
# SIMILARITY MATCHING LOOP
# ============================================================================

# Extract unique newsrooms from TikTok data
newsrooms <- unique(Tiktok_data$newsroom)

# Initialize progress tracking (separate file from Instagram/Facebook runs)
progress_file <- "processed_newsrooms_tiktok.txt"
if (file.exists(progress_file)) {
  processed_newsrooms <- readLines(progress_file)
} else {
  processed_newsrooms <- character()
}

# Initialize results dataframe or load existing partial results
results_file <- "final_results_Tiktok.rds"
if (file.exists(results_file)) {
  final_results_Tiktok <- readRDS(results_file)
} else {
  final_results_Tiktok <- data.frame(
    newsroom = character(),
    processed_description = character(),
    create_time = as.Date(character()),
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

  # Filter TikTok data for current newsroom
  new_Tiktok_data <- Tiktok_data %>%
    filter(newsroom == selected_newsroom & !is.na(processed_description) & processed_description != "")

  if (nrow(new_Tiktok_data) == 0) {
    print(paste("No valid data for newsroom:", selected_newsroom))
    next
  }

  # Load precomputed newsroom data (DTM, vocabulary, metadata)
  newsroom_data <- readRDS(file.path("newsroom_results", paste0(selected_newsroom, ".rds")))

  # Match TikTok posts to news articles using cosine similarity
  results <- match_some_posts(
    some_data = new_Tiktok_data,
    newsroom_data = newsroom_data,
    text_column = "processed_description",
    date_column = "create_time",
    similarity_threshold = 0.05,
    batch_size = 2000
  )

  # Prepare results: ensure data types and select columns
  results <- results %>%
    mutate(
      newsroom = selected_newsroom,
      create_time = as.Date(create_time),
      similarity_score = as.numeric(similarity_score)
    ) %>%
    select(newsroom, video_description, processed_description, like_count, create_time,
           similar_url, similarity_score, view_count, comment_count, share_count, id)

  # Append to results
  final_results_Tiktok <- bind_rows(final_results_Tiktok, results)

  # Save progress
  saveRDS(final_results_Tiktok, results_file)

  processed_newsrooms <- c(processed_newsrooms, selected_newsroom)
  writeLines(processed_newsrooms, progress_file)

  # Clean up memory
  rm(new_Tiktok_data, newsroom_data, results)
  gc()

  print(paste("Finished processing newsroom:", selected_newsroom))
}

print("All newsrooms have been processed.")

# ============================================================================
# POST-PROCESSING AND OUTPUT
# ============================================================================

# Join with news article metadata (headline, lead)
final_results_Tiktok <- final_results_Tiktok %>%
  left_join(alltexts_TP %>% select(url, headline, lead),
            by = c("similar_url" = "url"))

# Reorder columns for readability
final_results_Tiktok <- final_results_Tiktok %>%
  select(newsroom, processed_description, video_description, lead, headline,
         similar_url, similarity_score, everything())

# Export full results to RDS and Excel
saveRDS(final_results_Tiktok, results_file)
write_xlsx(final_results_Tiktok, "final_results_Tiktok.xlsx")

# Create sampled dataset: 10% per newsroom (minimum 20 posts) for manual review
sampled_results_Tiktok <- final_results_Tiktok %>%
  group_by(newsroom) %>%
  group_map(~ {
    num_rows <- nrow(.x)
    sample_size <- max(20, ceiling(num_rows * 0.1))
    .x %>% sample_n(sample_size)
  }) %>%
  bind_rows()

sampled_results_Tiktok <- ungroup(sampled_results_Tiktok)

print("Sampled dataframe created with 10% of rows for each newsroom (minimum 20 posts).")

# Export sampled results
write_xlsx(sampled_results_Tiktok, "sampled_results_Tiktok.xlsx")
