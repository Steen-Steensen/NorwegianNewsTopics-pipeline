# ============================================================================
# Script: 07_cosine_similarity_function.R
# Purpose: Match social media posts with most similar news stories using
#          cosine similarity, with date-based filtering (±3 days)
#
# Input:
#   - some_data: dataframe with social media posts
#     Required columns: text_column (post text), date_column (post date)
#   - newsroom_data: list containing preprocessed news data
#     Components: $dtm (document-term matrix), $dates, $urls
#
# Output:
#   - Enhanced dataframe with additional columns:
#     * similar_url: URL of best-matching news story
#     * similarity_score: cosine similarity score (0-1)
#
# Dependencies: Matrix, purrr, progress, text2vec
# ============================================================================

library(Matrix)
library(purrr)
library(progress)

# Resolve namespace conflict: ensure dplyr::select is used over data.table or other packages
select <- dplyr::select

match_some_posts <- function(some_data, newsroom_data, text_column, date_column,
                             similarity_threshold = 0.01, batch_size = 1000) {
  print("Matching posts using Cosine Similarity with date filtering...")

  # Extract vocabulary from newsroom DTM
  vectorizer_vocab <- colnames(newsroom_data$dtm)
  num_docs <- nrow(newsroom_data$dtm)

  # Normalize newsroom DTM rows by L2 norm for efficient cosine similarity
  dtm_norm <- newsroom_data$dtm / sqrt(rowSums(newsroom_data$dtm^2) + 1e-10)

  # Convert post dates to Date format for filtering
  some_data <- some_data %>%
    mutate(post_date = as.Date(get(date_column)))

  # Initialize progress bar
  pb <- progress_bar$new(
    total = nrow(some_data),
    format = "  Matching [:bar] :percent in :elapsed",
    clear = FALSE,
    width = 60
  )

  # Process each post and find best match
  results <- some_data %>%
    mutate(
      match_data = map(1:nrow(some_data), function(row_index) {
        pb$tick()

        post_text <- some_data[[text_column]][row_index]
        post_date <- some_data$post_date[row_index]

        # Filter newsroom documents to ±3 day window around post date
        valid_indices <- which(
          newsroom_data$dates >= (post_date - 3) &
            newsroom_data$dates <= (post_date + 3)
        )

        if (length(valid_indices) == 0) return(list(similar_url = NA, similarity_score = NA))

        # Subset normalized DTM and URLs to valid date range
        dtm_filtered <- dtm_norm[valid_indices, , drop = FALSE]
        urls_filtered <- newsroom_data$urls[valid_indices]

        # Tokenize post text and compute term frequencies
        post_tokens <- unlist(text2vec::word_tokenizer(tolower(post_text)))
        token_tf <- table(post_tokens)

        # Build TF-IDF weighted query vector from post text
        query_vector <- rep(0, length(vectorizer_vocab))
        matching_indices <- which(vectorizer_vocab %in% names(token_tf))

        if (length(matching_indices) > 0) {
          # Compute inverse document frequency weights
          idf_values <- log(1 + num_docs / (1 + colSums(newsroom_data$dtm > 0)))
          # Populate query vector with TF × IDF values
          query_vector[matching_indices] <- as.numeric(token_tf[vectorizer_vocab[matching_indices]]) * idf_values[matching_indices]
        }

        # Normalize query vector by L2 norm for cosine similarity
        query_vector <- query_vector / (sqrt(sum(query_vector^2)) + 1e-10)

        if (sum(query_vector) == 0 || any(is.na(query_vector))) return(list(similar_url = NA, similarity_score = NA))

        # Compute cosine similarity in batches to manage memory
        num_batches <- ceiling(nrow(dtm_filtered) / batch_size)
        best_match <- NULL
        best_similarity <- -Inf

        for (batch_idx in seq_len(num_batches)) {
          start_idx <- (batch_idx - 1) * batch_size + 1
          end_idx <- min(batch_idx * batch_size, nrow(dtm_filtered))
          dtm_batch <- dtm_filtered[start_idx:end_idx, , drop = FALSE]

          # Cosine similarity = normalized_dtm %*% query_vector
          similarities <- as.vector(dtm_batch %*% query_vector)
          batch_best_similarity <- max(similarities, na.rm = TRUE)

          if (batch_best_similarity > best_similarity) {
            best_similarity <- batch_best_similarity
            best_match <- which.max(similarities) + start_idx - 1
          }
        }

        # Return NA if no match exceeds similarity threshold
        if (is.null(best_match) || best_similarity < similarity_threshold) {
          return(list(similar_url = NA, similarity_score = NA))
        }

        best_match_url <- urls_filtered[best_match]
        return(list(similar_url = best_match_url, similarity_score = best_similarity))
      })
    ) %>%
    mutate(
      similar_url = map_chr(match_data, "similar_url"),
      similarity_score = map_dbl(match_data, "similarity_score")
    ) %>%
    select(-match_data)

  print("Matching completed.")
  return(results)
}
