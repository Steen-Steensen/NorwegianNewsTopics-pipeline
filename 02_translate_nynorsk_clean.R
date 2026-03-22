################################################################################
# TRANSLATION AND CLEANING SCRIPT: Translate Nynorsk to Bokmål, then Clean
################################################################################
#
# DESCRIPTION:
#   This script translates Norwegian Nynorsk articles to Bokmål using the
#   OpenAI API, then applies stopword removal and lemmatisation to ALL articles.
#   Performing cleaning after translation ensures that the Bokmål stopword list
#   and lemmatiser (lang = "nb") are applied only to Bokmål text, avoiding the
#   errors that would result from applying them directly to Nynorsk.
#
#   Translation uses raw full_text_NB (natural language text) rather than
#   pre-cleaned text, which gives the API better context and improves quality.
#   Texts are processed in parallel batches with exponential-backoff retry logic.
#
# INPUT:
#   - Remaining_nynorsk (dataframe or Remaining_nynorsk.rds): Nynorsk articles
#       from 01_preprocess_articles.R with columns:
#       * full_text_NB: raw Nynorsk text to translate
#       * url: unique identifier
#   - alltexts_original_pre_TP.rds: Preprocessed article dataset from script 01
#
# OUTPUT:
#   - Remaining_nynorsk (updated): dataframe with added column:
#       * translated_NB: translated Bokmål text (raw, pre-cleaning)
#   - Remaining_nynorsk.rds: Updated RDS checkpoint
#   - translation_progress.rds: Progress checkpoint (overwritten during processing)
#   - alltexts_preprocessed.rds: Final dataset with cleaned_text column:
#       * Bokmål articles: cleaned from full_text_NB directly
#       * Nynorsk articles: cleaned from their translated Bokmål text
#       Used as input by all downstream scripts (03+)
#
################################################################################

# Set OpenAI API key (replace with actual key before running)
openai_api_key <- "YOUR_OPENAI_API_KEY_HERE"

# Load required libraries
library(httr)
library(jsonlite)
library(furrr)
library(progress)
library(dplyr)
library(stringr)
library(pbapply)
library(tm)
library(textstem)
library(quanteda)

# Resolve namespace conflict: ensure dplyr::select is used over data.table or other packages
select <- dplyr::select

# Setup parallel processing plan
plan(multisession, workers = parallel::detectCores() - 1)

# ============================================================================
# 0. LOAD INPUTS
# ============================================================================

# Load Remaining_nynorsk if not already in memory (e.g. running script standalone)
if (!exists("Remaining_nynorsk")) {
  cat("Loading Remaining_nynorsk from Remaining_nynorsk.rds...\n")
  Remaining_nynorsk <- readRDS("Remaining_nynorsk.rds")
}

cat("Nynorsk articles to translate:", nrow(Remaining_nynorsk), "\n")

# ============================================================================
# RATE LIMIT HANDLING
# ============================================================================

# Function to handle API rate limit errors with exponential backoff
handle_rate_limit <- function(response, retries) {
  if (!is.null(response$status_code) && response$status_code == 429) {
    wait_time <- min(2^(retries + 1), 60)
    cat("Rate limit hit. Retrying in", wait_time, "seconds...\n")
    Sys.sleep(wait_time)
  }
}

# ============================================================================
# TRANSLATION FUNCTION
# ============================================================================

# Translate a single text via OpenAI API with retry logic
translate_single_text <- function(text, api_key, model_name) {
  url <- "https://api.openai.com/v1/chat/completions"

  body <- list(
    model = model_name,
    messages = list(
      list(role = "system", content = "Translate the following Norwegian Nynorsk text into Norwegian Bokmål, keeping context, punctuation, and stop words intact."),
      list(role = "user", content = text)
    ),
    max_tokens = 2000
  )

  max_retries <- 5
  retries <- 0

  while (retries < max_retries) {
    response <- tryCatch(
      POST(
        url,
        add_headers(Authorization = paste("Bearer", api_key)),
        content_type_json(),
        body = toJSON(body, auto_unbox = TRUE),
        timeout(300)
      ),
      error = function(e) {
        cat("Request failed:", conditionMessage(e), "\n")
        return(NULL)
      }
    )

    if (!is.null(response) && response$status_code == 200) {
      result <- content(response, as = "parsed")
      if (!is.null(result$choices)) {
        return(result$choices[[1]]$message$content)
      } else {
        cat("No choices in response.\n")
        return(NA)
      }
    } else {
      handle_rate_limit(response, retries)
      retries <- retries + 1
    }
  }

  cat("Max retries reached. Skipping text.\n")
  return(NA)
}

# ============================================================================
# BATCH TRANSLATION FUNCTION
# ============================================================================

batch_translate <- function(data, text_column, output_column, api_key, model_name,
                            batch_size = 10, save_progress_every = 10, save_file = "progress.rds") {
  total_texts <- nrow(data)
  num_batches <- ceiling(total_texts / batch_size)

  if (!output_column %in% colnames(data)) {
    data[[output_column]] <- rep(NA, total_texts)
  }

  pb <- progress_bar$new(
    format = "  Translating [:bar] :percent (~:eta remaining)",
    total = num_batches, clear = FALSE, width = 60
  )

  for (batch_num in seq_len(num_batches)) {
    batch_indices <- ((batch_num - 1) * batch_size + 1):min(batch_num * batch_size, total_texts)
    batch_texts <- data[[text_column]][batch_indices]

    if (all(!is.na(data[[output_column]][batch_indices]))) {
      pb$tick()
      next
    }

    batch_translations <- future_map(batch_texts,
                                     function(t) translate_single_text(t, api_key, model_name),
                                     .progress = FALSE)
    data[[output_column]][batch_indices] <- batch_translations

    if (batch_num %% save_progress_every == 0) {
      saveRDS(data, save_file)
      cat("Progress saved to", save_file, "\n")
    }

    pb$tick()
  }

  saveRDS(data, save_file)
  cat("Final progress saved to", save_file, "\n")

  return(data)
}

# ============================================================================
# EXECUTE TRANSLATION PIPELINE
# ============================================================================

api_key    <- openai_api_key
model_name <- "gpt-4o-mini"
batch_size <- 10

if (file.exists("translation_progress.rds")) {
  file.remove("translation_progress.rds")
  cat("Deleted existing translation_progress.rds file.\n")
}

# Translate raw full_text_NB (natural Nynorsk text) to Bokmål
# Using raw text gives the API better context than pre-cleaned/lemmatised text
Remaining_nynorsk <- batch_translate(
  data           = Remaining_nynorsk,
  text_column    = "full_text_NB",
  output_column  = "translated_NB",
  api_key        = api_key,
  model_name     = model_name,
  batch_size     = batch_size,
  save_progress_every = 5,
  save_file      = "translation_progress.rds"
)

# Save updated Remaining_nynorsk with translations
saveRDS(Remaining_nynorsk, "Remaining_nynorsk.rds")
cat("\nTranslation complete. Remaining_nynorsk saved to Remaining_nynorsk.rds\n")

# ============================================================================
# CLEAN AND LEMMATIZE ALL TEXTS
# ============================================================================

# Load the main article dataset from script 01
alltexts <- readRDS("alltexts_original_pre_TP.rds")
cat("Loaded", nrow(alltexts), "articles from alltexts_original_pre_TP.rds\n")

# Load Norwegian stopwords
norwegian_stopwords <- stopwords("no")

# Main text cleaning function:
# - Convert to lowercase
# - Remove punctuation and digits
# - Remove Norwegian stopwords
# - Lemmatize to base form using Bokmål lemmatiser
# - Remove extra whitespace
clean_text <- function(text) {
  text <- tolower(text)
  text <- str_replace_all(text, "[[:punct:]]", " ")
  text <- str_replace_all(text, "[[:digit:]]", " ")
  text <- removeWords(text, norwegian_stopwords)
  text <- lemmatize_strings(text, lang = "nb")
  text <- str_squish(text)
  return(text)
}

# For Nynorsk articles: use translated Bokmål text as the input to clean_text
# For all other articles: use full_text_NB (already Bokmål) as the input
alltexts <- alltexts %>%
  left_join(
    Remaining_nynorsk %>% select(url, translated_NB),
    by = "url"
  ) %>%
  mutate(text_to_clean = ifelse(!is.na(translated_NB), translated_NB, full_text_NB))

cat("\nCleaning and lemmatizing texts...\n")
alltexts$cleaned_text <- pblapply(alltexts$text_to_clean, clean_text)

# Remove TV2 boilerplate string that appears at the start of some articles
remove_string_if_early <- function(text) {
  words <- unlist(strsplit(text, "\\s+"))
  first_20 <- paste(words[1:min(20, length(words))], collapse = " ")
  if (grepl("siste nytt tip bilder video saken", first_20, ignore.case = TRUE)) {
    text <- sub("siste nytt tip bilder video saken\\s*", "", text, ignore.case = TRUE)
  }
  return(text)
}

alltexts$cleaned_text <- sapply(alltexts$cleaned_text, remove_string_if_early)
alltexts$cleaned_text <- unlist(alltexts$cleaned_text)

# Remove temporary working columns
alltexts$text_to_clean <- NULL
alltexts$translated_NB <- NULL

# ============================================================================
# SAVE FINAL PREPROCESSED DATASET
# ============================================================================

saveRDS(alltexts, "alltexts_preprocessed.rds")
cat("\nFinal preprocessed dataset saved to alltexts_preprocessed.rds\n")
cat("cleaned_text now contains Bokmål text for all articles\n")
cat("All downstream scripts (03+) should load alltexts_preprocessed.rds\n")

# NOTE: Per-newsroom Nynorsk translation for the social media similarity
# pipeline is handled separately in 09_translate_nynorsk_similarity.R.
